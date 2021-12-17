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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Hello world!
 */
@Slf4j
public class RedisServer {

    private final RedisConfiguration configuration;
    private EventLoopGroup boss = null;
    private EventLoopGroup workerGroup = null;
    private Channel serverChannel = null;

    public RedisServer(@NonNull RedisConfiguration configuration) {
        this.configuration = configuration;
    }

    public static void main(String[] args) {
        new RedisServer(new RedisConfiguration(6380, null)).start();
    }
    
    public void start() {
        int port = configuration.getPort();
        ServerBootstrap boot = new ServerBootstrap();
        RedisEngine engine = DefaultRedisEngine.defaultEngine();
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
        ChannelFuture channelFuture = boot.bind(port);
        channelFuture.syncUninterruptibly();
        serverChannel = channelFuture.channel();
        log.info("Redis start at port [{}] !", port);
    }

    public void stop() {
        if (serverChannel != null)
            serverChannel.close();
        if (boss != null) {
            boss.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        log.info("Redis server shutdown successfully !");
    }

}
