package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RandomElements;
import cn.deepmax.redis.utils.NumberUtils;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class RHash extends ScanMap<Key, Key> implements RedisObject, RandomElements {

    private LocalDateTime expire;
    private final TimeProvider provider;
    private final Random random = new Random();

    public RHash(TimeProvider timeProvider) {
        this.provider = timeProvider;
    }

    @Override
    public LocalDateTime expireTime() {
        return expire;
    }

    @Override
    public void expireAt(LocalDateTime time) {
        this.expire = time;
    }

    @Override
    public TimeProvider timeProvider() {
        return this.provider;
    }

    /**
     * add new value to hash
     *
     * @param pairs
     * @return
     */
    public int set(List<Pair> pairs) {
        if (pairs == null) return 0;
        for (Pair pair : pairs) {
            set(pair.field, pair.value);
        }
        return pairs.size();
    }

    public List<Pair> getAll() {
        return iterate(n -> new Pair(n.key, n.value));
    }

    public List<Key> keys() {
        return iterate(n -> n.key);
    }

    public List<Key> values() {
        return iterate(n -> n.value);
    }

    private <T> List<T> iterate(Function<Node<Key, Key>, T> mapper) {
        Node<Key, Key> cur = head;
        List<T> l = new ArrayList<>((int) size());
        while (cur != null) {
            l.add(mapper.apply(cur));
            cur = cur.next;
        }
        return l;
    }

    public int del(List<Key> fields) {
        int c = 0;
        for (Key it : fields) {
            Node<Key, Key> n = delete(it);
            if (n != null) c++;
        }
        return c;
    }

    public Double incrByFloat(Key field, Double incr) {
        Key v = get(field);
        Double old;
        if (v == null) {
            old = 0D;
        } else {
            old = NumberUtils.parseDouble(v.str());
        }
        Double newV = old + incr;
        set(field, new Key(NumberUtils.formatDouble(newV).getBytes(StandardCharsets.UTF_8)));
        return newV;
    }

    public Long incrBy(Key field, Long incr) {
        Key v = get(field);
        Long old;
        if (v == null) {
            old = 0L;
        } else {
            old = NumberUtils.parseNumber(v.str());
        }
        Long newV = old + incr;
        set(field, new Key(newV.toString().getBytes(StandardCharsets.UTF_8)));
        return newV;
    }

    public List<Pair> randField(long count) {
        return randomCount(count, this::getAll);
    }

    public static class Pair {
        public final Key field;
        public final Key value;

        public Pair(Key field, Key value) {
            this.field = field;
            this.value = value;
        }

        public Pair(byte[] field, byte[] value) {
            this(new Key(field), new Key(value));
        }
    }
}
