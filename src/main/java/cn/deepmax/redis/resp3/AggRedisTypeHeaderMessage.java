package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.ArrayHeaderRedisMessage;

public class AggRedisTypeHeaderMessage extends ArrayHeaderRedisMessage {
    private final RedisMessageType type;

    public AggRedisTypeHeaderMessage(long length, RedisMessageType type) {
        super(length);
        this.type = type;
    }

    public RedisMessageType getType() {
        return type;
    }

}
