package cn.deepmax.redis.engine;

import cn.deepmax.redis.infra.DefaultTimeProvider;
import cn.deepmax.redis.infra.TimeProvider;

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
    private final Map<String, RedisValue> map = new ConcurrentHashMap<>();

    private RedisEngine() {
        timeProvider = new DefaultTimeProvider();
    }

    public void setTimeProvider(TimeProvider timeProvider) {
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    public boolean del(String key) {
        RedisValue value = map.remove(key);
        return value != null && !value.expired();
    }


    public void set(String key, String value) {
        map.put(key, new RedisString(value, timeProvider));
    }

    public Optional<String> get(String key) {
        RedisValue v = map.get(key);
        if (v != null) {
            return Optional.of(((RedisString) v).getS());
        }
        return Optional.empty();
    }


}
