package cn.deepmax.redis.api;

import cn.deepmax.redis.core.DbKey;
import cn.deepmax.redis.core.Key;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

/**
 * @author wudi
 * @date 2021/5/20
 */
public interface DbManager {

    RedisEngine engine();

    default RedisEngine.Db get(Redis.Client client) {
        return get(getIndex(client));
    }

    RedisEngine.Db get(int index);

    int getIndex(Redis.Client client);

    void switchTo(Redis.Client client, int index);

    int getTotal();

    void fireChangeQueuedEvents(Redis.Client client);

    void fireChangeEvents(Redis.Client client, List<KeyEvent> events);

    default void fireChangeEvent(Redis.Client client, KeyEvent event) {
        if (event != null) {
            this.fireChangeEvents(client, Collections.singletonList(event));
        }
    }

    void addListener(Redis.Client client, List<Key> keys, KeyEventListener listener);

    void removeListener(KeyEventListener listener);

    @FunctionalInterface
    interface KeyEventListener {

        void accept(List<KeyEvent> modified, KeyEventListener listener);
    }

    enum EventType {
        DEL, NEW_OR_REPLACE, UPDATE, EXPIRE
    }

    class KeyEvent extends DbKey {
        public final EventType type;
        public final LocalDateTime time;

        public KeyEvent(byte[] content, int db, EventType type, LocalDateTime time) {
            super(content, db);
            this.type = type;
            this.time = time;
        }

        public KeyEvent(byte[] content, int db, EventType type) {
            this(content, db, type, LocalDateTime.now());
        }

    }
}
