package cn.deepmax.redis.engine;

public interface RedisEngine  {

    RedisObject set(byte[] key, RedisObject newValue);

    RedisObject get(byte[] key);

    RedisObject del(byte[] key);

    boolean isExpire(RedisObject obj);
}
