package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

import java.math.BigDecimal;

/**
 * @author wudi
 * @date 2021/12/3
 */
public class DoubleRedisMessage implements RedisMessage {

    public static final DoubleRedisMessage INF = new DoubleRedisMessage(null);
    public static final DoubleRedisMessage INF_NEG = new DoubleRedisMessage(null);
    private final BigDecimal value;

    public DoubleRedisMessage(BigDecimal value) {
        this.value = value;
    }

    public Double getValue() {
        if (this == INF) {
            return Double.POSITIVE_INFINITY;
        } else if (this == INF_NEG) {
            return Double.NEGATIVE_INFINITY;
        }
        return value.doubleValue();
    }

    public boolean isInf() {
        return this == INF;
    }

    public boolean isInfNeg() {
        return this == INF_NEG;
    }

    public BigDecimal getExValue() {
        return value;
    }

    public String content() {
        if (this == INF) {
            return Constants.INF;
        } else if (this == INF_NEG) {
            return "-" + Constants.INF;
        } else {
            return value.toString();
        }
    }
}
