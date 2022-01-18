package cn.deepmax.redis;

import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.args.Args;
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
import java.util.function.Function;

/**
 * Hello world!
 */
@Slf4j
public class RedisServer {

    private EventLoopGroup boss = null;
    private EventLoopGroup workerGroup = null;
    private List<ChannelFuture> binds = new ArrayList<>();
    private final RedisEngine engine;

    public RedisServer(@NonNull RedisEngine engine, RedisConfiguration configuration) {
        configuration.check();
        this.engine = engine;
        this.engine.setConfiguration(configuration);
    }
    
    static Args.Flag<String> F_AUTH = Args.Flag.newInstance("a", null, Function.identity());
    static Args.Flag<String> F_CLUSTER_AUTH = Args.Flag.newInstance("ca", null, Function.identity());
    static Args.Flag<Integer> F_PORT = Args.Flag.newInstance("p", "6381", Integer::new);
    static Args.Flag<String> F_H = Args.Flag.newInstance("h", "localhost", Function.identity());

    public static void main(String[] args) {
        new Args().flag(F_AUTH)
                .flag(F_CLUSTER_AUTH)
                .flag(F_H)
                .flag(F_PORT).parse(args);

        RedisConfiguration.Standalone standalone = new RedisConfiguration.Standalone(F_PORT.get(), F_AUTH.get());
        RedisConfiguration.Cluster cluster = new RedisConfiguration.Cluster(F_AUTH.get(), Arrays.asList(
                new RedisConfiguration.Node("m1", 6391)
                        .appendSlave(new RedisConfiguration.Node("s1", 6394)),
                new RedisConfiguration.Node("m2", 6392)
                        .appendSlave(new RedisConfiguration.Node("s2", 6395)),
                new RedisConfiguration.Node("m3", 6393)
                        .appendSlave(new RedisConfiguration.Node("s3", 6396))
        ));
        new RedisServer(DefaultRedisEngine.defaultEngine(), new RedisConfiguration(F_H.get(), standalone, cluster)).start();
    }

    private RedisConfiguration configuration() {
        return engine.configuration();
    }

    public void startWithShutdownHook() {
        start();
        Thread hookThread = new Thread(this::stop);
        hookThread.setName("Embed-redis shutdownHook");
        Runtime.getRuntime().addShutdownHook(hookThread);
    }

    public void start() {
        ServerBootstrap boot = new ServerBootstrap();

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
                                .addLast(new RedisResp3Encoder())
                                .addLast(new RedisServerHandler(engine));
                    }
                });
        //to bind ports
        StringBuilder logText = new StringBuilder();
        logText.append("Redis started ");
        RedisConfiguration configuration = configuration();
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
        binds.forEach(ChannelFuture::syncUninterruptibly);
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
