package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Module;
import io.netty.handler.codec.redis.RedisMessage;

public interface RedisEngine {

    void loadModule(Module module);
    
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
