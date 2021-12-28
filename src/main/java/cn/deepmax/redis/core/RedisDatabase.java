package cn.deepmax.redis.core;

import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/5/20
 */
public class RedisDatabase implements RedisEngine.Db {

    private final Map<Key, RedisObject> data = new ConcurrentHashMap<>();
    private final DbManager manager;
    private final int idx;

    public RedisDatabase(DbManager manager, int idx) {
        this.manager = manager;
        this.idx = idx;
    }

    @Override
    public RedisObject set(Redis.Client client, byte[] key, RedisObject newValue) {
        getDbManager().fireChangeEvent(client, new DbManager.KeyEvent(key, selfIndex(), DbManager.EventType.NEW_OR_REPLACE));
        return data.put(new Key(key), newValue);
    }

    @Override
    public void multiSet(Redis.Client client, List<Key> keys, List<RedisObject> newValues) {
        for (int i = 0; i < keys.size(); i++) {
            data.put(keys.get(i), newValues.get(i));
        }
        List<DbManager.KeyEvent> events = keys.stream()
                .map(k -> new DbManager.KeyEvent(k.getContent(), selfIndex(), DbManager.EventType.NEW_OR_REPLACE))
                .collect(Collectors.toList());
        getDbManager().fireChangeEvents(client, events);
    }

    @Override
    public DbManager getDbManager() {
        return manager;
    }

    @Override
    public int selfIndex() {
        return idx;
    }


    @Override
    public RedisObject get(Redis.Client client, byte[] key) {
        Key key1 = new Key(key);
        RedisObject obj = data.get(key1);
        if (obj != null && obj.isExpire()) {
            onExpire(client, key);
            data.remove(key1);
            return null;
        }
        return obj;
    }

    private void onExpire(Redis.Client client, byte[] key) {
        getDbManager().fireChangeEvent(client, new DbManager.KeyEvent(key, selfIndex(), DbManager.EventType.EXPIRE));
    }

    @Override
    public RedisObject del(Redis.Client client, byte[] key) {
        RedisObject remove = data.remove(new Key(key));
        if (remove == null) {
            return null;
        }
        if (remove.isExpire()) {
            onExpire(client, key);
            return null;
        }
        getDbManager().fireChangeEvent(client, new DbManager.KeyEvent(key, selfIndex(), DbManager.EventType.DEL));
        return remove;
    }

    @Override
    public void flush() {
        data.clear();
    }
}
