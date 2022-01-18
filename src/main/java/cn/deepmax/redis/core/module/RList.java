package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RedisDataType;
import cn.deepmax.redis.core.Sized;
import cn.deepmax.redis.core.support.AbstractRedisObject;

import java.util.*;
import java.util.function.Function;

public class RList extends AbstractRedisObject implements Sized {

    private final LinkedList<Key> list = new LinkedList<>();

    public RList(TimeProvider timeProvider) {
        super(timeProvider);
    }

    @Override
    public Type type() {
        return new RedisDataType("list","linkedlist");
    }
    
    @Override
    public RedisObject copyTo(Key key) {
        RList copy = new RList(this.timeProvider);
        copy.list.addAll(this.list);
        return copy;
    }

    public void lpush(Key data) {
        Objects.requireNonNull(data);
        list.addFirst(data);
    }

    public void rpush(Key data) {
        Objects.requireNonNull(data);
        list.addLast(data);
    }

    @Override
    public long size() {
        return list.size();
    }

    public Key lPop() {
        return list.removeFirst();
    }

    public Key rPop() {
        return list.removeLast();
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

    public List<Key> lrange(int start, int end) {
        start = tranStart(start);
        end = tranEnd(end);
        if (start > end) {
            return Collections.emptyList();
        }
        Iterator<Key> it = list.iterator();
        //skip start length
        List<Key> result = new ArrayList<>();
        int skip = start;
        int counter = 0;
        while (it.hasNext()) {
            Key next = it.next();
            if (skip > 0) {
                skip--;
            } else {
                result.add(next);
            }
            counter++;
            if (counter > end) {
                break;
            }
        }
        return result;
    }

    public List<Integer> lpos(byte[] ele) {
        return lpos(ele, Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * @param ele
     * @param rankO   not eq 0
     * @param countO
     * @param maxlenO
     * @return
     */
    public List<Integer> lpos(byte[] ele, Optional<Long> rankO, Optional<Long> countO, Optional<Long> maxlenO) {
        //rank can be negative
        long rank = rankO.orElse(1L);
        boolean neg = rank < 0;
        long shouldSkip = rank >= 0 ? rank - 1 : -(1 + rank); //skip elements
        long count = countO.orElse(1L);
        if (count == 0L) count = size();                    //0 : find all
        long maxLen = maxlenO.orElse(size());
        if (maxLen == 0L) maxLen = size();
        List<Integer> result = new ArrayList<>();
        Iterator<Key> it = (neg ? list.descendingIterator() : list.iterator());
        long pos = (neg ? size() - 1 : 0);              //current pointer
        while (it.hasNext() && maxLen > 0) {
            Key cur = it.next();
            if (Arrays.equals(ele, cur.getContent())) {
                if (shouldSkip > 0L) {
                    shouldSkip--;
                } else {
                    result.add((int) pos);
                    count--;
                    if (count == 0L) return result;
                }
            }
            //next
            maxLen--;
            if (neg) pos--;
            else pos++;
        }
        return result;
    }

    /**
     * @param pivot
     * @param ele
     * @param offset
     * @return -1 if pivot not found or length after insert
     */
    public int insert(byte[] pivot, byte[] ele, int offset) {
        //find pivot
        int c = 0;
        Iterator<Key> it = list.iterator();
        while (it.hasNext()) {
            Key v = it.next();
            if (Arrays.equals(v.getContent(), pivot)) {
                //pivot is at pos c
                break;
            }
            c++;
        }
        if (c == size()) {
            return -1;
        }
        list.add(c + offset, new Key(ele));
        return list.size();
    }

    public Key valueAt(int idx) {
        int len = list.size();
        if (idx < 0) idx += len;
        if (idx >= 0 && idx < len) {
            Iterator<Key> it = list.iterator();
            Key v = null;
            while (idx >= 0) {
                v = it.next();
                idx--;
            }
            return v;
        } else {
            return null;
        }
    }

    /**
     * @param idx
     * @param ele
     * @return -1 if out of range
     */
    public int lset(int idx, byte[] ele) {
        int len = list.size();
        if (idx < 0) idx += len;
        if (idx >= 0 && idx < len) {
            list.set(idx, new Key(ele));
            return 1;
        } else {
            return -1;
        }
    }

    /**
     * count gt 0: Remove elements equal to element moving from head to tail.
     * count lt 0: Remove elements equal to element moving from tail to head.
     * count eq 0: Remove all elements equal to element.
     *
     * @param ele
     * @param count numbers
     * @return
     */
    public int remove(byte[] ele, int count) {
        boolean neg = count < 0;
        int len = list.size();
        if (count == 0) count = len;
        count = Math.abs(count);
        //to at most remove count
        Iterator<Key> it = neg ? list.descendingIterator() : list.iterator();
        int c = 0;
        while (it.hasNext()) {
            Key v = it.next();
            if (Arrays.equals(v.getContent(), ele)) {
                it.remove();
                c++;
                count--;
                if (count == 0) break;
            }
        }
        return c;
    }

    /**
     * @param start
     * @param stop
     */
    public void trim(int start, int stop) {
        start = tranStart(start);
        stop = tranEnd(stop);
        if (start > stop) {
            list.clear();
            return;
        }
        int tailToRemove = list.size() - 1 - stop;
        int headToRemove = start;
        while (headToRemove-- > 0) list.removeFirst();
        while (tailToRemove-- > 0) list.removeLast();
    }
}
