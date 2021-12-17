package cn.deepmax.redis.type;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.NonNull;

import java.util.function.Consumer;

/**
 * @author wudi
 * @date 2021/12/17
 */
public interface CallbackRedisMessage extends RedisMessage {
    RedisMessage unwrap();

    static Impl of(RedisMessage msg) {
        return new Impl(msg);
    }

    /**
     * callback after write
     *
     * @param action
     */
    void addHook(Consumer<ChannelHandlerContext> action);

    void callback(ChannelHandlerContext ctx);

    class Impl implements CallbackRedisMessage {

        RedisMessage content;
        Consumer<ChannelHandlerContext> action;

        public Impl(@NonNull RedisMessage content) {
            this.content = content;
        }

        @Override
        public RedisMessage unwrap() {
            return content;
        }

        @Override
        public void callback(ChannelHandlerContext ctx) {
            if (action != null) {
                action.accept(ctx);
            }
        }

        @Override
        public void addHook(Consumer<ChannelHandlerContext> action) {
            this.action = action;
        }

    }
}
