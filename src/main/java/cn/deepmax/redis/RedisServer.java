package cn.deepmax.redis;

import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisEngineHolder;
import cn.deepmax.redis.core.DefaultRedisEngine;
import cn.deepmax.redis.netty.RedisServerHandler;
import cn.deepmax.redis.resp3.RedisAggTypesAggregator;
import cn.deepmax.redis.resp3.RedisBulkValueAggregator;
import cn.deepmax.redis.resp3.RedisResp3Decoder;
import cn.deepmax.redis.resp3.RedisResp3Encoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 */
@Slf4j
public class RedisServer {
    private final RedisConfiguration configuration;
    private EventLoopGroup boss = null;
    private EventLoopGroup workerGroup = null;
    private List<ChannelFuture> binds = new ArrayList<>();
    private final RedisEngine engine;

    public RedisServer(@NonNull RedisEngine engine, RedisConfiguration configuration) {
        this.engine = engine;
        this.configuration = configuration;
        this.configuration.check();
    }

    public static void main(String[] args) {
        String auth = "123456";
        RedisConfiguration.Standalone standalone = new RedisConfiguration.Standalone(6380, auth);
        RedisConfiguration.Cluster cluster = new RedisConfiguration.Cluster(auth, Arrays.asList(
                new RedisConfiguration.Node("m1", 6391)
                        .appendSlave(new RedisConfiguration.Node("s1", 6394)),
                new RedisConfiguration.Node("m2", 6392)
                        .appendSlave(new RedisConfiguration.Node("s2", 6395)),
                new RedisConfiguration.Node("m3", 6393)
                        .appendSlave(new RedisConfiguration.Node("s3", 6396))
        ));
        new RedisServer(DefaultRedisEngine.defaultEngine(), new RedisConfiguration("localhost",standalone, cluster)).start();
    }

    public void start() {
        ServerBootstrap boot = new ServerBootstrap();
        engine.setConfiguration(configuration);
        RedisEngineHolder.set(engine);

        boss = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(1);
        boot.group(boss, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new RedisResp3Decoder())
                                .addLast(new RedisBulkValueAggregator())
                                .addLast(new RedisAggTypesAggregator())
//                                .addLast(redisTypeDecoder)
                                .addLast(new RedisResp3Encoder())
                                .addLast(new RedisServerHandler(engine));

                    }
                });
        //to bind ports
        StringBuilder logText = new StringBuilder();
        logText.append("Redis started ");
        if (configuration.getStandalone() != null) {
            binds.add(boot.bind(configuration.getStandalone().getPort()));
            logText.append("with standalone port ").append(configuration.getStandalone().getPort()).append(" ");
        }
        if (configuration.getCluster() != null) {
            logText.append(", with cluster port: ");
            List<RedisConfiguration.Node> nodes = configuration.getCluster().getAllNodes();
            for (int i = 0; i < nodes.size(); i++) {
                binds.add(boot.bind(nodes.get(i).port));
                logText.append(nodes.get(i).port);
                if (i < nodes.size() - 1) {
                    logText.append(",");
                }
            }
        }
        binds.forEach(i -> i.syncUninterruptibly());
        log.info("{} !", logText.toString());
    }
    
    public void stop() {
        for (ChannelFuture ch : binds) {
            try {
                ch.channel().close();
                log.info("Redis server shutdown for address: {}",ch.channel().localAddress());
            } catch (Exception e) {
                log.error("Close error ", e);
            }
        }
        if (boss != null) {
            boss.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        log.info("Redis server shutdown successfully !");
    }

}
