package cn.deepmax.redis.core;

import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wudi
 * @date 2021/5/20
 */
public class RedisDatabase implements RedisEngine.Db {

    private final Map<Key, RedisObject> data = new ConcurrentHashMap<>();

    @Override
    public RedisObject set(byte[] key, RedisObject newValue) {
        return data.put(new Key(key), newValue);
    }
    
    @Override
    public RedisObject get(byte[] key) {
        Key key1 = new Key(key);
        RedisObject obj = data.get(key1);
        if (obj != null && obj.isExpire()) {
            data.remove(key1);
            return null;
        }
        return obj;
    }

    @Override
    public RedisObject del(byte[] key) {
        RedisObject remove = data.remove(new Key(key));
        if (remove != null && remove.isExpire()) {
            return null;
        }
        return remove;
    }

    @Override
    public void flush() {
        data.clear();
    }
}
