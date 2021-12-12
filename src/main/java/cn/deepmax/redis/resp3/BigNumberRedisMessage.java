package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

import java.math.BigDecimal;

/**
 * @author wudi
 * @date 2021/12/3
 */
public class BigNumberRedisMessage implements RedisMessage {
    private final BigDecimal value;

    public BigNumberRedisMessage(BigDecimal value) {
        this.value = value;
    }

    public BigDecimal getValue() {
        return value;
    }

    public String content() {
        return value.toString();
    }
}
