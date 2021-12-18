package cn.deepmax.redis.netty;

import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.NettyClient;
import cn.deepmax.redis.lua.LuaChannelContext;
import cn.deepmax.redis.type.CallbackRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import io.netty.channel.ChannelFuture;
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
            if (response instanceof CompositeRedisMessage) {
                for (RedisMessage child : ((CompositeRedisMessage) response).children()) {
                    doWriteMessage(ctx, child);
                }
            } else {
                doWriteMessage(ctx, response);
            }
        }
    }

    private void doWriteMessage(ChannelHandlerContext ctx, RedisMessage response) {
        ChannelFuture future = ctx.writeAndFlush(response);
        if (response instanceof CallbackRedisMessage) {
            CallbackRedisMessage m = (CallbackRedisMessage) response;
            future.addListener(e -> m.callback(ctx));
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
        log.info("Channel exit {}", ctx.channel().remoteAddress());
        engine.pubsub().quit(new NettyClient(ctx.channel()));
    }

}
