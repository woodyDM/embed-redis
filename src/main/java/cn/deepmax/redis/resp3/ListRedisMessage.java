package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

public class ListRedisMessage extends ArrayRedisMessage implements RedisMessage {
    public ListRedisMessage(List<RedisMessage> children) {
        super(children);
    }

}
