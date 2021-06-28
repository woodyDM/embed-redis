package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisType;

public interface RedisEngine {

    RedisConfiguration configuration();

    void setConfiguration(RedisConfiguration configuration);

    RedisType execute(RedisType type, Redis.Client client);
    
    DbManager getDbManager();
    
    AuthManager authManager();

    PubsubManager pubsub();

    boolean isExpire(RedisObject obj);

    interface Db {

        RedisObject set(byte[] key, RedisObject newValue);

        RedisObject get(byte[] key);

        RedisObject del(byte[] key);
        
    }
    
}
