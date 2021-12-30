package cn.deepmax.redis.core.support;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/4/30
 */
public abstract class AbstractRedisObject implements RedisObject {

    private LocalDateTime expire;
    protected final TimeProvider timeProvider;

    public AbstractRedisObject(TimeProvider timeProvider) {
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
