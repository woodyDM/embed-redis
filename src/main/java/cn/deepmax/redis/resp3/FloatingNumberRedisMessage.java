package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/12/3
 */
public class FloatingNumberRedisMessage implements RedisMessage {

    private final Double value;

    public static final FloatingNumberRedisMessage INF = new FloatingNumberRedisMessage(Double.POSITIVE_INFINITY);
    public static final FloatingNumberRedisMessage INF_NEG = new FloatingNumberRedisMessage(Double.NEGATIVE_INFINITY);

    public FloatingNumberRedisMessage(Double value) {
        this.value = value;
    }

    public Double getValue() {
        return value;
    }
}
