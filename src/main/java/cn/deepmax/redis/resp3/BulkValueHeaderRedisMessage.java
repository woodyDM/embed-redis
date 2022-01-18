package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 */
public class BulkValueHeaderRedisMessage implements RedisMessage {
    private final int length;
    private final RedisMessageType type;

    public BulkValueHeaderRedisMessage(int length, RedisMessageType type) {
        this.length = length;
        this.type = type;
    }

    public RedisMessageType getType() {
        return type;
    }

    public int getLength() {
        return length;
    }
}
