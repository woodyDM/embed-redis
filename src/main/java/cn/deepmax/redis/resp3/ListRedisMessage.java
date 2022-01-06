package cn.deepmax.redis.resp3;

import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.Key;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ListRedisMessage extends ArrayRedisMessage implements RedisMessage {

    public ListRedisMessage(List<RedisMessage> children) {
        super(children);
    }
    
    public static ListRedisMessage wrapKeys(List<Key> list){
        if (list == null || list.isEmpty()) {
            return empty();
        }
        List<RedisMessage> r = list.stream().map(k -> FullBulkValueRedisMessage.ofString(k.getContent()))
                .collect(Collectors.toList());
        return new ListRedisMessage(r);
    }

    public static ListRedisMessage empty() {
        return new ListRedisMessage(Collections.emptyList());
    }

    public FullBulkValueRedisMessage getAt(int i) {
        if (i < 0 || i >= children().size()) {
            throw new RedisServerException("ERR wrong number of arguments ");
        }
        return (FullBulkValueRedisMessage) children().get(i);
    }

    public static ListRedisMessage ofString(String s) {
        Builder b = newBuilder();
        for (String it : s.split(" ")) {
            if (it != null && it.length() > 0) {
                b.append(FullBulkValueRedisMessage.ofString(it));
            }
        }
        return b.build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        List<RedisMessage> l = new ArrayList<>();

        public Builder append(RedisMessage msg) {
            l.add(msg);
            return this;
        }

        public Builder append(String msg) {
            l.add(FullBulkValueRedisMessage.ofString(msg));
            return this;
        }

        public Builder append(byte[] msg) {
            l.add(FullBulkValueRedisMessage.ofString(msg));
            return this;
        }
        
        public ListRedisMessage build() {
            return new ListRedisMessage(l);
        }
    }
}
