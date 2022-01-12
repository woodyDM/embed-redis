package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RedisDataType;
import cn.deepmax.redis.utils.Tuple;

import java.time.LocalDateTime;
import java.util.*;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class SortedSet extends ZSet<Double, Key> implements RedisObject {

    protected LocalDateTime expire;
    protected final TimeProvider timeProvider;
    private final Key selfKey;

    public SortedSet(TimeProvider timeProvider, Key key) {
        this.timeProvider = timeProvider;
        this.selfKey = key;
    }
    
    @Override
    public Type type() {
        return new RedisDataType("zset", "skiplist");
    }

    @Override
    public RedisObject copyTo(Key key) {
        SortedSet copy = new SortedSet(this.timeProvider, key);
        copy.add(this.toPairs());
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

    public static final int ZADD_OUT_NOP = 1;
    public static final int ZADD_OUT_UPDATED = 1 << 1;
    public static final int ZADD_OUT_ADDED = 1 << 2;

    /**
     * @param score
     * @param ele
     * @param nx
     * @param xx
     * @param gt
     * @param lt
     * @param incr
     * @return empty if NAN. when with value: t.a flags , t.b inre newscore
     */
    public Optional<Tuple<Integer, Double>> zadd(Double score, Key ele, boolean nx, boolean xx,
                                                 boolean gt, boolean lt, boolean incr) {
        if (score.isNaN()) {
            return Optional.empty();
        }
        Double old = dict.get(ele);
        if (old != null) {
            /* NX? Return, same element already exists. */
            if (nx) {
                return Optional.of(new Tuple<>(ZADD_OUT_NOP, null));
            }
            /* Prepare the score for the increment if needed. */
            if (incr) {
                score = old + score;
                if (score.isNaN()) {
                    return Optional.empty();
                }
            }
            /* GT/LT? Only update if score is greater/less than current. */
            if ((gt && score <= old) || (lt && score >= old)) {
                return Optional.of(new Tuple<>(ZADD_OUT_NOP, null));
            }
            if (!score.equals(old)) {
                zsl.updateScore(ele, old, score);
                dict.set(ele, score);
                return Optional.of(new Tuple<>(ZADD_OUT_UPDATED, score));
            }
            return Optional.of(new Tuple<>(ZADD_OUT_NOP, score));
        } else if (!xx) {
            zsl.insert(score, ele);
            dict.set(ele, score);
            return Optional.of(new Tuple<>(ZADD_OUT_ADDED, score));
        } else {
            return Optional.of(new Tuple<>(ZADD_OUT_NOP, null));
        }
    }

    /**
     * union
     *
     * @param arg
     * @param sets
     * @return
     */
    public static SortedSet union(TimeProvider time, Key dest, SortedSetModule.ComplexArg arg, List<SortedSet> sets) {
        Map<Key, Double> scores = new HashMap<>();
        for (SortedSet oneSet : sets) {
            oneSet.dict.forEach((k, s) -> {
                double value = s * arg.getWeight(oneSet.selfKey);
                if (Double.isNaN(value)) value = 0D;
                Double old = scores.get(k);
                if (old == null) {
                    //not exist ,put
                    scores.put(k, value);
                } else {
                    Double newValue = arg.type.agg(old, value);
                    scores.put(k, newValue);
                }
            });
        }
        // create set
        SortedSet result = new SortedSet(time, dest);
        scores.forEach((k, s) -> result.add(s, k));
        return result;
    }

    /**
     * inter
     *
     * @param arg
     * @param sets
     * @return
     */
    public static SortedSet inter(TimeProvider time, Key dest, SortedSetModule.ComplexArg arg, List<SortedSet> sets) {
        sets.sort(Comparator.comparing(SortedSet::size));

        Map<Key, Double> scores = new HashMap<>();
        SortedSet minSet = sets.get(0);
        for (Map.Entry<Key, ScanMap.Node<Key, Double>> entry : minSet.dict.entrySet()) {
            Key key = entry.getKey();
            boolean inter = true;
            Double base = entry.getValue().value * arg.getWeight(minSet.selfKey);
            for (int i = 1; i < sets.size(); i++) {
                SortedSet oneSet = sets.get(i);
                Double otherValue = oneSet.dict.get(key);
                if (otherValue == null) {
                    inter = false;
                    break;
                }
                otherValue = otherValue * arg.getWeight(oneSet.selfKey);
                if (otherValue.isNaN()) otherValue = 0D;
                base = arg.type.agg(base, otherValue);
            }
            if (inter) scores.put(key, base);
        }
        
        if (scores.isEmpty()) {
            return null;
        }
        // create set
        SortedSet result = new SortedSet(time, dest);
        scores.forEach((k, s) -> result.add(s, k));
        return result;
    }

}
