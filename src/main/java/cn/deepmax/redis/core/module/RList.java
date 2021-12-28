package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.AbstractRedisObject;

import java.util.LinkedList;
import java.util.Objects;

/**
 *
 */
public class RList extends AbstractRedisObject {

    private final LinkedList<Key> list = new LinkedList<>();

    public RList(TimeProvider timeProvider) {
        super(timeProvider);
    }

    public void addFirst(Key data) {
        Objects.requireNonNull(data);
        list.addFirst(data);
    }

    public long size() {
        return list.size();
    }

    public Key lPop() {
        if (size() == 0) {
            return null;
        } else {
            return list.removeFirst();
        }
    }
}
