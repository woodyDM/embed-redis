package cn.deepmax.redis.api;

import cn.deepmax.redis.core.DbKey;
import cn.deepmax.redis.core.Key;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

public interface DbManager extends Flushable {

    RedisEngine engine();

    default RedisEngine.Db get(Client client) {
        return get(getIndex(client));
    }

    RedisEngine.Db get(int index);

    int getIndex(Client client);

    void switchTo(Client client, int index);

    int getTotal();

    int listenerSize();

    void fireChangeQueuedEvents(Client client);

    void fireChangeEvents(Client client, List<KeyEvent> events);

    default void fireChangeEvent(Client client, KeyEvent event) {
        if (event != null) {
            this.fireChangeEvents(client, Collections.singletonList(event));
        }
    }

    void addListener(Client client, List<Key> keys, KeyEventListener listener);

    void removeListener(KeyEventListener listener);

    enum EventType {
        DEL, NEW_OR_REPLACE, UPDATE, EXPIRE
    }

    @FunctionalInterface
    interface KeyEventListener {

        void accept(List<KeyEvent> modified, KeyEventListener listener);
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
