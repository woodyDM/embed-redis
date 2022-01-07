package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class SortedSet extends ZSet<Double, Key> implements RedisObject {


    protected LocalDateTime expire;
    protected final TimeProvider timeProvider;
    private final Key key;

    public SortedSet(TimeProvider timeProvider, Key key) {
        this.timeProvider = timeProvider;
        this.key = key;
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

    /**
     * union
     *
     * @param arg
     * @param sets
     * @return
     */
    public static SortedSet union(TimeProvider time, SortedSetModule.ComplexArg arg, List<SortedSet> sets) {
        Map<Key, Double> scores = new HashMap<>();
        for (SortedSet oneSet : sets) {
            oneSet.dict.forEach((k, s) -> {
                double value = s * arg.getWeight(oneSet.key);
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
        SortedSet result = new SortedSet(time, Key.DUMMY);
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
    public static SortedSet inter(TimeProvider time, SortedSetModule.ComplexArg arg, List<SortedSet> sets) {
        sets.sort(Comparator.comparing(SortedSet::size));

        Map<Key, Double> scores = new HashMap<>();
        SortedSet minSet = sets.get(0);
        for (Map.Entry<Key, ScanMap.Node<Key, Double>> entry : minSet.dict.entrySet()) {
            Key key = entry.getKey();
            boolean inter = true;
            Double base = entry.getValue().value * arg.getWeight(minSet.key);
            for (int i = 1; i < sets.size(); i++) {
                SortedSet oneSet = sets.get(i);
                Double otherValue = oneSet.dict.get(key);
                if (otherValue == null) {
                    inter = false;
                    break;
                }
                otherValue = otherValue * arg.getWeight(oneSet.key);
                if (otherValue.isNaN()) otherValue = 0D;
                base = arg.type.agg(base, otherValue);
            }
            if (inter) scores.put(key, base);
        }
        
        if (scores.isEmpty()) {
            return null;
        }
        // create set
        SortedSet result = new SortedSet(time, Key.DUMMY);
        scores.forEach((k, s) -> result.add(s, k));
        return result;
    }

}
