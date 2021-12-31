package cn.deepmax.redis.utils;

/**
 * @author wudi
 * @date 2021/12/31
 */
public class Range<T extends Comparable<T>> {
    T start;
    T end;
    boolean startOpen;
    boolean endOpen;

    public Range(T start, T end, boolean startOpen, boolean endOpen) {
        this.start = start;
        this.end = end;
        this.startOpen = startOpen;
        this.endOpen = endOpen;
    }

    Range() {
    }

    public T getStart() {
        return start;
    }

    public T getEnd() {
        return end;
    }

    public boolean isStartOpen() {
        return startOpen;
    }

    public boolean isEndOpen() {
        return endOpen;
    }

}
