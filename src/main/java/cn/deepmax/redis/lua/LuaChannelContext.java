package cn.deepmax.redis.lua;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.FastThreadLocal;

/**
 * it seems no way to add channel to lua function.
 * we only use one thread for redis
 * this thread also run the lua script, so use threadLocal  to save current lua channel.
 * @author wudi
 * @date 2021/5/26
 */
public class LuaChannelContext {
    public static final FastThreadLocal<ChannelHandlerContext> CHANNEL = new FastThreadLocal<>();

    public static void set(ChannelHandlerContext channel) {
        CHANNEL.set(channel);
    }

    public static ChannelHandlerContext get() {
        return CHANNEL.get();
    }

    public static void remove() {
        CHANNEL.remove();
    }
}
