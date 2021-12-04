package cn.deepmax.redis.resp3;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

import java.nio.charset.StandardCharsets;

public class FullBulkValueRedisMessage extends FullBulkStringRedisMessage {
    private final RedisMessageType type;

    public FullBulkValueRedisMessage(ByteBuf content, RedisMessageType type) {
        super(content);
        this.type = type;
    }

    public RedisMessageType type() {
        return type;
    }

    @Override
    public String toString() {
        return content().toString(StandardCharsets.UTF_8);
    }

}
