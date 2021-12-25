package cn.deepmax.redis.api;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/4/30
 */
public interface RedisObject {

    LocalDateTime expireTime();

    long ttl();

    long pttl();

    void expire(long ttl);

    void pexpire(long pttl);

    boolean isExpire();

    void expireAt(LocalDateTime time);

    void persist();

}
