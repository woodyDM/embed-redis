package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.core.module.NavMap;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/5/20
 */
public class RedisDatabase implements RedisEngine.Db {

    private final NavMap<RedisObject> data = new NavMap<>();
    private final DbManager manager;
    private final int idx;

    public RedisDatabase(DbManager manager, int idx) {
        this.manager = manager;
        this.idx = idx;
    }

    @Override
    public Object getContainer() {
        return data;
    }

    @Override
    public RedisObject set(Client client, byte[] key, RedisObject newValue) {
        getDbManager().fireChangeEvent(client, new DbManager.KeyEvent(key, selfIndex(), DbManager.EventType.NEW_OR_REPLACE));
        return data.set(key, newValue);
    }

    @Override
    public void multiSet(Client client, List<Key> keys, List<RedisObject> newValues) {
        for (int i = 0; i < keys.size(); i++) {
            data.set(keys.get(i), newValues.get(i));
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
    public RedisObject get(Client client, byte[] key) {
        Key key1 = new Key(key);
        RedisObject obj = data.get(key1);
        if (obj != null && obj.isExpire()) {
            onExpire(client, key);
            data.delete(key1);
            return null;
        }
        return obj;
    }

    private void onExpire(Client client, byte[] key) {
        getDbManager().fireChangeEvent(client, new DbManager.KeyEvent(key, selfIndex(), DbManager.EventType.EXPIRE));
    }

    @Override
    public RedisObject del(Client client, byte[] key) {
        NavMap.Node<RedisObject> node = data.delete(key);
        if (node == null) {
            return null;
        }
        RedisObject ele = node.getValue();
        if (ele.isExpire()) {
            onExpire(client, key);
            return null;
        }
        getDbManager().fireChangeEvent(client, new DbManager.KeyEvent(key, selfIndex(), DbManager.EventType.DEL));
        return ele;
    }

    @Override
    public void flush() {
        data.clear();
    }
}
