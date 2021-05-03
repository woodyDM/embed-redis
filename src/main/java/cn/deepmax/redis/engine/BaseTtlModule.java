package cn.deepmax.redis.engine;

import cn.deepmax.redis.infra.DefaultTimeProvider;
import cn.deepmax.redis.infra.TimeProvider;
import lombok.NonNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BaseTtlModule<E extends TtlObject> extends BaseModule {
    public BaseTtlModule(String name) {
        super(name);
    }

    protected TimeProvider timeProvider = new DefaultTimeProvider();
    protected final Map<Key, E> data = new ConcurrentHashMap<>();

    @Override
    public void setTimeProvider(TimeProvider timeProvider) {
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    public E set(byte[] key, E newValue) {
        return data.put(new Key(key), newValue);
    }

    public E get(byte[] key) {
        return data.get(new Key(key));
    }

    public E del(byte[] key) {
        return data.remove(new Key(key));
    }

    protected boolean expired(@NonNull E v) {
        LocalDateTime time = v.expireTime();
        return time != null && timeProvider.now().isAfter(time);
    }

    static class Key {
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
