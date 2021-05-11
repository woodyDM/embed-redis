package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisType;

public interface RedisEngine  {

    RedisObject set(byte[] key, RedisObject newValue);

    RedisObject get(byte[] key);

    RedisObject del(byte[] key);

    boolean isExpire(RedisObject obj);

    RedisCommand getCommand(RedisType type);

    RedisExecutor executor();

    AuthManager authManager();
    
    PubsubManager pubsub();
    
}
