package cn.deepmax.redis.core;

import java.util.*;

/**
 * @author wudi
 */
public class DbKey extends Key {
    public final int db;

    public DbKey(byte[] content, int db) {
        super(content);
        this.db = db;
    }

    /**
     * support child class equals .
     *
     * @param o
     * @return equals
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!super.equals(o)) return false;
        DbKey dbKey = (DbKey) o;
        return db == dbKey.db;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), db);
    }

    /**
     * decode intersect for listeners
     *
     * @param l1 list 1
     * @param l2 list 2
     * @param <A> generic
     * @param <B> generic
     * @return intersect result
     */
    public static <A extends DbKey, B extends DbKey> boolean intersect(List<A> l1, List<B> l2) {
        Set<A> s = new HashSet<>(l1);
        return l2.stream().anyMatch(s::contains);
    }

    /**
     * filter duplicated event
     *
     * @param list list
     * @param <A> generic
     * @return distinct keys
     */
    public static <A extends DbKey> List<A> compress(List<A> list) {
        Set<A> set = new HashSet<>();
        List<A> result = new ArrayList<>();
        for (A ele : list) {
            if (set.contains(ele)) {
                continue;
            }
            result.add(ele);
            set.add(ele);
        }
        return result;
    }
}
