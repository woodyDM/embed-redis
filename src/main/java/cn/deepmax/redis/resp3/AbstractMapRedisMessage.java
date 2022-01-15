package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractMapRedisMessage extends AbstractReferenceCounted implements RedisMessage {

    private final Map<RedisMessage, RedisMessage> data = new LinkedHashMap<>();
    private final List<RedisMessage> children;

    protected AbstractMapRedisMessage() {
        this(null);
    }

    public AbstractMapRedisMessage(List<RedisMessage> list) {
        if (list != null) {
            int len = list.size();
            if (len % 2 == 1) {
                throw new IllegalStateException("can't create map");
            }
            for (int i = 0; i < len / 2; i++) {
                data.put(list.get(i * 2), list.get(i * 2 + 1));
            }
            children = Collections.unmodifiableList(list);
        } else {
            children = Collections.emptyList();
        }
    }

    public Map<RedisMessage, RedisMessage> content() {
        return data;
    }

    public List<RedisMessage> children() {
        return children;
    }

    @Override
    protected void deallocate() {
        data.forEach((k, v) -> {
            ReferenceCountUtil.release(k);
            ReferenceCountUtil.release(v);
        });
    }

    public Map<RedisMessage, RedisMessage> data() {
        return data;
    }

    public RedisMessage get(RedisMessage redisMessage) {
        return data.get(redisMessage);
    }

    public int size() {
        return data.size();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        data.forEach((k, v) -> {
            ReferenceCountUtil.touch(k);
            ReferenceCountUtil.touch(v);
        });
        return this;
    }
}
