package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 */
public class NullRedisMessage implements RedisMessage {

    public static final NullRedisMessage INSTANCE = new NullRedisMessage();

    private NullRedisMessage() {
    }
}
