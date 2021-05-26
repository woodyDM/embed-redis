package cn.deepmax.redis.engine;

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
        return data.get(new Key(key));
    }

    @Override
    public RedisObject del(byte[] key) {
        return data.remove(new Key(key));
    }

    
    
   

    @Override
    public void addKeyListener(RedisEngine.KeyListener keyListener) {

    }
}