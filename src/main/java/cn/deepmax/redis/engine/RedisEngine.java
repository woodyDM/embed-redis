package cn.deepmax.redis.engine;

import cn.deepmax.redis.infra.DefaultTimeProvider;
import cn.deepmax.redis.infra.TimeProvider;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class RedisEngine {

    private static final RedisEngine S = new RedisEngine();

    public static RedisEngine getInstance() {
        return S;
    }
    
    private TimeProvider timeProvider;
    private final Map<Key, RedisValue> map = new ConcurrentHashMap<>();

    private RedisEngine() {
        timeProvider = new DefaultTimeProvider();
    }

    public void setTimeProvider(TimeProvider timeProvider) {
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    public boolean del(byte[] key) {
        RedisValue value = map.remove(new Key(key));
        return value != null && !value.expired();
    }
    
    public void set(byte[] key, byte[] value) {
        map.put(new Key(key), new InRedisString(value, timeProvider));
    }

    public Optional<byte[]> get(byte[] key) {
        RedisValue v = map.get(new Key(key));
        if (v != null) {
            return Optional.of(((InRedisString) v).getS());
        }
        return Optional.empty();
    }

    static class Key{
        byte[] content;

        Key(byte[] content) {
            this.content = content;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Arrays.equals(content, key.content);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(content);
        }
    }


}
