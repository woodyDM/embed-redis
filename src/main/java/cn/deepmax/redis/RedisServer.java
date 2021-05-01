package cn.deepmax.redis;

import cn.deepmax.redis.engine.RedisEngine;
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
import io.netty.handler.codec.redis.RedisEncoder;

/**
 * Hello world!
 */
public class RedisServer {

    private EventLoopGroup boss = null;
    private EventLoopGroup workerGroup = null;
    private Channel serverChannel = null;

    private int port;

    public static void main(String[] args) {
        new RedisServer(6379).start();
        
    }
    public RedisServer(int port) {
        this.port = port;
    }

    public void start() {
        ServerBootstrap boot = new ServerBootstrap();
        RedisEngine engine = RedisEngine.getInstance();
        boss = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(4);
        boot.group(boss, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new RedisDecoder(true))
                                .addLast(new RedisDecoder())
                                .addLast(new RedisBulkStringAggregator())
                                .addLast(new RedisArrayAggregator())
                                .addLast(new RedisEncoder())
                                .addLast(new RedisServerHandler(engine));
                    }
                });
        ChannelFuture channelFuture = boot.bind(port);
        channelFuture.syncUninterruptibly();
        serverChannel = channelFuture.channel();
        System.out.println("Redis start at port "+port);
       
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
    }

}
