package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/12/3
 */
public class BooleanRedisMessage implements RedisMessage {

    public static final BooleanRedisMessage TRUE = new BooleanRedisMessage(true);
    public static final BooleanRedisMessage FALSE = new BooleanRedisMessage(false);
    private final boolean value;

    private BooleanRedisMessage(boolean value) {
        this.value = value;
    }

    public boolean value() {
        return value;
    }

    public String content() {
        return value ? "t" : "f";
    }
}
