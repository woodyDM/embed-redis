package cn.deepmax.redis.resp3;

import cn.deepmax.redis.api.RedisParamException;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

public class ListRedisMessage extends ArrayRedisMessage implements RedisMessage {
    public ListRedisMessage(List<RedisMessage> children) {
        super(children);
    }

    public FullBulkValueRedisMessage getAt(int i) {
        if (i < 0 || i >= children().size()) {
            throw new RedisParamException("ERR wrong number of arguments ");
        }
        return (FullBulkValueRedisMessage) children().get(i);
    }

}
