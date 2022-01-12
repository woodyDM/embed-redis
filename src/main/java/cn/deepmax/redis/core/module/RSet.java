package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RandomElements;
import cn.deepmax.redis.core.RedisDataType;

import java.time.LocalDateTime;
import java.util.*;

public class RSet extends ScanMap<Key, Boolean> implements RedisObject, RandomElements {
    private LocalDateTime expire;
    private final TimeProvider timeProvider;

    public RSet(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
    }
    
    @Override
    public Type type() {
        return new RedisDataType("set", "hashtable");
    }

    @Override
    public RedisObject copyTo(Key key) {
        RSet copy = new RSet(this.timeProvider);
        copy.add(this.members());
        return copy;
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

    /**
     * @param members
     * @return removed count
     */
    public int remove(List<Key> members) {
        int c = 0;
        for (Key it : members) {
            if (delete(it) != null) c++;
        }
        return c;
    }

    public boolean remove(Key member) {
        return delete(member) != null;
    }

    public List<Key> members() {
        return new ArrayList<>(container.keySet());
    }

    public List<Key> diff(List<RSet> sets) {
        List<Key> all = members();
        if (sets == null || sets.isEmpty()) return all;
        for (RSet other : sets) {
            Iterator<Key> it = all.iterator();
            while (it.hasNext()) {
                Key member = it.next();
                if (other.get(member) != null) it.remove();
                if (all.isEmpty()) return Collections.emptyList();
            }
        }
        return all;
    }

    public static List<Key> inter(List<RSet> sets) {
        if (sets == null || sets.isEmpty()) return Collections.emptyList();
        if (sets.size() == 1) return sets.get(0).members();
        sets.sort(Comparator.comparing(ScanMap::size));
        List<Key> base = sets.get(0).members();
        List<Key> result = new ArrayList<>(base.size());
        for (Key member : base) {
            boolean inter = true;
            for (int i = 1; i < sets.size(); i++) {
                RSet other = sets.get(i);
                if (other.get(member) == null) {
                    inter = false;
                    break;
                }
            }
            if (inter) result.add(member);
        }
        return result;
    }

    public static List<Key> union(List<RSet> sets) {
        if (sets == null || sets.isEmpty()) return Collections.emptyList();
        if (sets.size() == 1) return sets.get(0).members();
        Set<Key> keys = new HashSet<>();
        for (RSet set : sets) {
            keys.addAll(set.members());
        }
        return new ArrayList<>(keys);
    }
}
