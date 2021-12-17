package cn.deepmax.redis.api;

import io.netty.handler.codec.redis.RedisMessage;

public interface RedisEngine {
    
    void dataFlush();

    void scriptFlush();

    RedisConfiguration configuration();

    void setConfiguration(RedisConfiguration configuration);

    RedisMessage execute(RedisMessage type, Redis.Client client);

    DbManager getDbManager();

    AuthManager authManager();

    PubsubManager pubsub();

    boolean isExpire(RedisObject obj);

    interface Db {

        RedisObject set(byte[] key, RedisObject newValue);

        RedisObject get(byte[] key);

        RedisObject del(byte[] key);

        void flush();

    }

}
