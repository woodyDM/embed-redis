package cn.deepmax.redis.base;

import cn.deepmax.redis.utils.Tuple;

import java.util.function.Supplier;

/**
 * @author wudi
 * @date 2022/1/6
 */
public interface BlockTest {
    
    default <T> Tuple<Long, T> block(Supplier<T> action) {
        long start = System.nanoTime();
        T v = action.get();
        long cost = System.nanoTime() - start;
        long mills = cost / 1000_000;
        return new Tuple<>(mills, v);
    }
    
}
