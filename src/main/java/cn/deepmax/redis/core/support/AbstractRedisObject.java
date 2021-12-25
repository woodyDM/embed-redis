package cn.deepmax.redis.core.support;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;

import java.time.Duration;
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
    public long ttl() {
        if (expire == null) {
            return -1L;
        } else {
            Duration d = Duration.between(timeProvider.now(), expire);
            return d.getSeconds() < 0 ? -2L : d.getSeconds();
        }
    }

    @Override
    public void expire(long ttl) {
        this.expire = timeProvider.now().plusSeconds(ttl);
    }

    @Override
    public boolean isExpire() {
        return expire != null && timeProvider.now().isAfter(expire);
    }

    @Override
    public long pttl() {
        if (expire == null) {
            return -1L;
        } else {
            Duration d = Duration.between(timeProvider.now(), expire);
            return d.toMillis() < 0 ? -2L : d.toMillis();
        }
    }

    @Override
    public void expireAt(LocalDateTime time) {
        this.expire = time;
    }

    @Override
    public void pexpire(long pttl) {
        this.expire = timeProvider.now().plusNanos(pttl * 1000_000);
    }

    @Override
    public void persist() {
        this.expire = null;
    }

}
