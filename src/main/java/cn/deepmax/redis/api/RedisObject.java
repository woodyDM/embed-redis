package cn.deepmax.redis.api;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/4/30
 */
public interface RedisObject {

    LocalDateTime expireTime();

    void expireAt(LocalDateTime time);

    TimeProvider timeProvider();

    default long ttl() {
        LocalDateTime expire = expireTime();
        if (expire == null) {
            return -1L;
        } else {
            Duration d = Duration.between(timeProvider().now(), expire);
            return d.getSeconds() < 0 ? -2L : d.getSeconds();
        }
    }

    default long pttl() {
        if (expireTime() == null) {
            return -1L;
        } else {
            Duration d = Duration.between(timeProvider().now(), expireTime());
            return d.toMillis() < 0 ? -2L : d.toMillis();
        }
    }

    default void expire(long ttl) {
        LocalDateTime time = timeProvider().now().plusSeconds(ttl);
        this.expireAt(time);
    }

    default void pexpire(long pttl) {
        this.expireAt(timeProvider().now().plusNanos(pttl * 1000_000));
    }

    default boolean isExpire() {
        return expireTime() != null && timeProvider().now().isAfter(expireTime());
    }
    
    default void persist() {
        expireAt(null);
    }
}
