package cn.deepmax.redis.netty;

import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.NettyClient;
import cn.deepmax.redis.lua.LuaChannelContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Slf4j
public class RedisServerHandler extends ChannelInboundHandlerAdapter {

    private final RedisEngine engine;

    public RedisServerHandler(RedisEngine engine) {
        this.engine = engine;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RedisMessage type = (RedisMessage) msg;
        RedisMessage response;
        try {
            LuaChannelContext.set(ctx);
            response = engine.execute(type, new NettyClient(ctx.channel()));
        } finally {
            ReferenceCountUtil.release(type);
            LuaChannelContext.remove();
        }
        if (response != null) {
            ctx.writeAndFlush(response);
        } else {
            log.error("Empty response found! Input is {}", type);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.print("exceptionCaught: ");
        cause.printStackTrace(System.err);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Exit channel ");
        engine.pubsub().quit(new NettyClient(ctx.channel()));
    }

}
