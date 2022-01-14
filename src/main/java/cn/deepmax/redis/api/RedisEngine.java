package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.Module;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.Set;

public interface RedisEngine extends Flushable {

    Statistic statistic();

    void loadModule(Module module);

    TimeProvider timeProvider();

    RedisConfiguration configuration();

    void setConfiguration(RedisConfiguration configuration);

    RedisMessage execute(RedisMessage type, Client client);

    DbManager getDbManager();

    default RedisEngine.Db getDb(Client client) {
        return getDbManager().get(client);
    }

    default void fireChangeEvent(Client client, byte[] key, DbManager.EventType type) {
        int index = getDbManager().getIndex(client);
        getDbManager().fireChangeEvent(client, new DbManager.KeyEvent(key, index, type));
    }

    AuthManager authManager();

    AuthManager clusterAuthManager();

    default AuthManager clientAuthManager(Client client) {
        return client.node().isPresent() ? clusterAuthManager() : authManager();
    }

    PubsubManager pubsub();

    TransactionManager transactionManager();

    CommandManager commandManager();

    interface Db extends Flushable {

        long size();

        Key randomKey();

        Set<Key> keys();

        Object getContainer();

        DbManager getDbManager();

        int selfIndex();

        RedisObject set(Client client, byte[] key, RedisObject newValue);

        void multiSet(Client client, List<Key> keys, List<RedisObject> newValues);

        RedisObject get(Client client, byte[] key);

        RedisObject del(Client client, byte[] key);

    }

}
