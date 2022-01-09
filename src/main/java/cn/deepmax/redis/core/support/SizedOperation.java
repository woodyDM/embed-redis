package cn.deepmax.redis.core.support;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Sized;

public interface SizedOperation {
    /**
     * remove set which size = 0 or fire update event
     *
     * @param key
     * @param engine
     * @param client
     */
    default void deleteEleIfNeed(byte[] key, RedisEngine engine, Client client) {
        Sized afterSet = (Sized) engine.getDb(client).get(client, key);
        if (afterSet.size() == 0) {
            engine.getDb(client).del(client, key);
        } else {
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
        }
    }
}
