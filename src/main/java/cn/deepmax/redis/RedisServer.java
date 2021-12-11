package cn.deepmax.redis;

import cn.deepmax.redis.core.DefaultRedisEngine;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisEngineHolder;
import cn.deepmax.redis.netty.RedisEncoder;
import cn.deepmax.redis.netty.RedisServerHandler;
import cn.deepmax.redis.netty.RedisTypeDecoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Hello world!
 */
@Slf4j
public class RedisServer {

    private EventLoopGroup boss = null;
    private EventLoopGroup workerGroup = null;
    private Channel serverChannel = null;
    private final RedisConfiguration configuration;

    public RedisServer(@NonNull RedisConfiguration configuration) {
        this.configuration = configuration;
    }

    public static void main(String[] args) {
        new RedisServer(new RedisConfiguration(6380, null)).start();
    }


    public void start() {
        int port = configuration.getPort();
        ServerBootstrap boot = new ServerBootstrap();
        RedisEngine engine = DefaultRedisEngine.instance();
        RedisEngineHolder.set(engine);
        engine.authManager().setAuth(configuration.getAuth());
        engine.setConfiguration(configuration);
        
        RedisTypeDecoder redisTypeDecoder = new RedisTypeDecoder();
        boss = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(1);
        boot.group(boss, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new RedisDecoder(true))
                                .addLast(new RedisBulkStringAggregator())
                                .addLast(new RedisArrayAggregator())
                                .addLast(redisTypeDecoder)
                                .addLast(new RedisEncoder())
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
