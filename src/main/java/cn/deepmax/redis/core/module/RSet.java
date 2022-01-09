package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RandomElements;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class RSet extends ScanMap<Key, Boolean> implements RedisObject, RandomElements {
    private LocalDateTime expire;
    private final TimeProvider timeProvider;

    public RSet(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
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
        return timeProvider;
    }

    public int add(List<Key> members) {
        if (members == null || members.isEmpty()) {
            return 0;
        }
        int c = 0;
        for (Key it : members) {
            Boolean v = get(it);
            if (v == null) {
                c++;
                set(it, true);
            }
        }
        return c;
    }

    public List<Key> randomMember(long count) {
        return randomCount(count, () -> new ArrayList<>(container.keySet()));
    }
}
