package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

public class SetRedisMessage extends ArrayRedisMessage {
    public SetRedisMessage(List<RedisMessage> children) {
        super(children);
    }
}
