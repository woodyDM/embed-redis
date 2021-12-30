package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class SortedSet extends ZSet<Double, Key> implements RedisObject {

    protected LocalDateTime expire;
    protected final TimeProvider timeProvider;

    public SortedSet(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
    }

    @Override
    public LocalDateTime expireTime() {
        return expire;
    }

    @Override
    public void expireAt(LocalDateTime time) {
        this.expire = time;
    }

    @Override
    public TimeProvider timeProvider() {
        return timeProvider;
    }
    
}
