package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/12/3
 */
public class NullRedisMessage implements RedisMessage {

    public static final NullRedisMessage INS = new NullRedisMessage();

    private NullRedisMessage() {
    }
}
