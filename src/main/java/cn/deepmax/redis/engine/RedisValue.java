package cn.deepmax.redis.engine;

/**
 * @author wudi
 * @date 2021/4/30
 */
public interface RedisValue {

    boolean expired();

    long ttl();

    void setTtl(long ttl);

}
