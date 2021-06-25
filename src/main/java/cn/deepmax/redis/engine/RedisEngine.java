package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisType;

public interface RedisEngine {

    RedisConfiguration configuration();

    void setConfiguration(RedisConfiguration configuration);

    DbManager getDbManager();
    
    RedisCommand getCommand(RedisType type);

    DefaultRedisExecutor executor();

    AuthManager authManager();

    PubsubManager pubsub();

    boolean isExpire(RedisObject obj);

    interface Db {

        RedisObject set(byte[] key, RedisObject newValue);

        RedisObject get(byte[] key);

        RedisObject del(byte[] key);
        
        void addKeyListener(KeyListener keyListener);

    }

    interface KeyListener {
        /**
         * key动作监听
         *
         * @param key
         * @param obj    变化后的key
         * @param opType 1 set  -1 del
         */
        void op(int db, byte[] key, RedisObject obj, int opType);
    }

}
