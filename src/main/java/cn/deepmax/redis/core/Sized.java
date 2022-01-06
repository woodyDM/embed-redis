package cn.deepmax.redis.core;

/**
 * object with size
 */
public interface Sized {

    long size();

    default long trimToSize(long count) {
        if (count > size()) {
            return size();
        } else {
            return count;
        }
    }

    default int tranStart(int start) {
        int len = (int) size();
        if (start < 0) start = len + start;
        if (start < 0) start = 0;
        return start;
    }

    /**
     * @param end
     * @return [0, size-1]
     */
    default int tranEnd(int end) {
        int len = (int) size();
        if (end < 0) end = len + end;
        if (end < 0) end = 0;
        if (end >= len) end = len - 1;
        return end;
    }
}
