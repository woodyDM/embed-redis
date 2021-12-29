package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.AbstractRedisObject;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 *
 */
public class RList extends AbstractRedisObject {

    private final LinkedList<Key> list = new LinkedList<>();

    public RList(TimeProvider timeProvider) {
        super(timeProvider);
    }

    public void lpush(Key data) {
        Objects.requireNonNull(data);
        list.addFirst(data);
    }

    public void rpush(Key data) {
        Objects.requireNonNull(data);
        list.addLast(data);
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

    public Key rPop() {
        if (size() == 0) {
            return null;
        } else {
            return list.removeLast();
        }
    }

    public List<Key> lPop(int count) {
        return doPop(count, LinkedList::removeFirst);
    }

    public List<Key> rPop(int count) {
        return doPop(count, LinkedList::removeLast);
    }

    private List<Key> doPop(int count, Function<LinkedList<Key>, Key> f) {
        List<Key> result = new ArrayList<>();
        int s = Math.min(count, list.size());
        for (int i = 0; i < s; i++) {
            result.add(f.apply(list));
        }
        return result;
    }
}
