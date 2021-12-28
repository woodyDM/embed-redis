package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.Module;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.stream.Collectors;

public interface RedisEngine {

    void loadModule(Module module);

    void dataFlush();

    void scriptFlush();

    TimeProvider timeProvider();

    RedisConfiguration configuration();

    void setConfiguration(RedisConfiguration configuration);

    RedisMessage execute(RedisMessage type, Redis.Client client);

    DbManager getDbManager();

    default RedisEngine.Db getDb(Redis.Client client) {
        return getDbManager().get(client);
    }

    default void fireChangeEvent(Redis.Client client, byte[] key, DbManager.EventType type) {
        int index = getDbManager().getIndex(client);
        getDbManager().fireChangeEvent(client, new DbManager.KeyEvent(key, index, type));
    }

    default void fireChangeEvents(Redis.Client client, List<Key> keys, DbManager.EventType type) {
        int index = getDbManager().getIndex(client);
        List<DbManager.KeyEvent> events = keys.stream().map(k -> new DbManager.KeyEvent(k.getContent(), index, type)).collect(Collectors.toList());
        getDbManager().fireChangeEvents(client, events);
    }

    AuthManager authManager();

    PubsubManager pubsub();

    TransactionManager transactionManager();

    CommandManager commandManager();

    interface Db {

        DbManager getDbManager();

        int selfIndex();

        RedisObject set(Redis.Client client, byte[] key, RedisObject newValue);

        void multiSet(Redis.Client client, List<Key> keys, List<RedisObject> newValues);

        RedisObject get(Redis.Client client, byte[] key);

        RedisObject del(Redis.Client client, byte[] key);

        void flush();

    }

}
