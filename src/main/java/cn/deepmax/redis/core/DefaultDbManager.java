package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisServerException;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author wudi
 */
@Slf4j
public class DefaultDbManager implements DbManager {

    private static final AttributeKey<Integer> IDX = AttributeKey.valueOf("DB_INDEX");
    private static final AttributeKey<List<KeyEvent>> EVENTS = AttributeKey.valueOf("EVENTS");
    private final RedisEngine engine;
    private final int total;
    private final RedisEngine.Db[] dbs;
    //for simplify , use List for listeners.
    private final LinkedList<ListenerWrapper> listeners = new LinkedList<>();

    public DefaultDbManager(RedisEngine engine, int total) {
        this.engine = engine;
        this.total = total;
        this.dbs = new RedisDatabase[total];
        for (int i = 0; i < total; i++) {
            dbs[i] = new RedisDatabase(this, i);
        }
    }

    @Override
    public RedisEngine engine() {
        return engine;
    }

    @Override
    public RedisEngine.Db get(int index) {
        if (index >= 0 && index < total) {
            return dbs[index];
        }
        throw new RedisServerException("db number out of bound");
    }

    @Override
    public void switchTo(Client client, int index) {
        if (index >= 0 && index < total) {
            client.channel().attr(IDX).set(index);
        } else {
            throw new RedisServerException("db number out of bound");
        }
    }

    @Override
    public int getIndex(Client client) {
        Attribute<Integer> attr = client.channel().attr(IDX);
        attr.setIfAbsent(0);
        return attr.get();
    }

    @Override
    public int getTotal() {
        return total;
    }

    @Override
    public void fireChangeEvents(Client client, List<KeyEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        Attribute<List<KeyEvent>> att = client.channel().attr(EVENTS);
        att.setIfAbsent(new ArrayList<>());
        List<KeyEvent> list = att.get();
        list.addAll(events);
    }

    @Override
    public int listenerSize() {
        return listeners.size();
    }

    @Override
    public void fireChangeQueuedEvents(Client client) {
        Attribute<List<KeyEvent>> attr = client.channel().attr(EVENTS);
        List<KeyEvent> list = attr.get();
        if (list == null || list.isEmpty()) {
            log.debug("No events for client {}", client);
            return;
        }
        List<KeyEvent> events = DbKey.compress(list);
        if (!events.isEmpty()) {
            doFireChangeEvents(events);
            log.debug("Fire [{}] events for client {} ", events.size(), client);
            attr.set(null);
        }
    }

    private void doFireChangeEvents(List<KeyEvent> events) {
        for (ListenerWrapper w : listeners) {
            if (KeyEvent.intersect(events, w.list)) {
                w.listener.accept(events, w.listener);
            }
        }
    }

    @Override
    public void addListener(Client client, List<Key> keys, KeyEventListener listener) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        Objects.requireNonNull(listener);
        int index = getIndex(client);
        List<DbKey> list = keys.stream()
                .map(k -> new DbKey(k.getContent(), index))
                .collect(Collectors.toList());
        listeners.addLast(new ListenerWrapper(list, listener));
    }

    @Override
    public void removeListener(KeyEventListener listener) {
        listeners.removeIf(w -> w.listener == listener);
    }

    static class ListenerWrapper {
        final List<DbKey> list;
        final KeyEventListener listener;

        public ListenerWrapper(List<DbKey> list, KeyEventListener listener) {
            this.list = list;
            this.listener = listener;
        }
    }

    @Override
    public void flush() {
        listeners.clear();
    }
}
