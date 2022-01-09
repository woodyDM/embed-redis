package cn.deepmax.redis.core;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

public interface RandomElements extends Sized {

    Random RAND = new Random();

    default <T> List<T> randomCount(long count, Supplier<List<T>> all) {
        List<T> list = all.get();
        if (count > 0) {
            Collections.shuffle(list);
            if (count >= size()) {
                return list;
            } else {
                return list.subList(0, (int) count);
            }
        } else {
            List<T> r = new LinkedList<>();
            int len = (int) size();
            for (int i = 0; i < -count; i++) {
                r.add(list.get(RAND.nextInt(len)));
            }
            return r;
        }
    }

}
