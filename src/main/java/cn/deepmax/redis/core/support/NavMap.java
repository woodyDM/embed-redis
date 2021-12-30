package cn.deepmax.redis.core.support;

import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RPattern;

import java.util.*;

/**
 * @author wudi
 * @date 2021/7/2
 */
public class NavMap<T> {
    private final Map<Key, Node<T>> container = new HashMap<>();
    private final Map<Long, Node<T>> numberContainer = new HashMap<>();
    Node<T> tail;
    Node<T> head;
    private long preIndex = 0L;

    public T get(byte[] k) {
        return this.get(new Key(k));
    }

    public T get(Key key) {
        Node<T> n = container.get(key);
        return n == null ? null : n.value;
    }

    public Node<T> get(Long idx) {
        return numberContainer.get(idx);
    }

    public int size() {
        return container.size();
    }

    public Node<T> delete(byte[] key) {
        return delete(new Key(key));
    }

    public Node<T> delete(Key key) {
        Node<T> old = container.remove(key);
        if (old == null) {
            return null;
        }
        if (old == tail) {
            tail = old.pre;
        }
        removeOld(old);
        return old;
    }

    private void removeOld(Node<T> old) {
        if (old == head) {
            head = head.next;
        }
        if (old.pre != null) {
            old.pre.next = old.next;
        }
        if (old.next != null) {
            old.next.pre = old.pre;
        }
        old.next = null;
        old.pre = null;
        numberContainer.remove(old.idx);
    }

    public T set(byte[] k, T t) {
        return this.set(new Key(k), t);
    }

    public T set(Key key, T t) {
        preIndex++;
        //append node to last
        Node<T> node = new Node<>(key, t, preIndex);
        node.pre = tail;
        if (tail != null) {
            tail.next = node;
        }
        tail = node;
        if (head == null) {
            head = node;
        }

        numberContainer.put(preIndex, node);
        //remove old if need
        Node<T> old = container.put(key, node);
        if (old != null) {
            removeOld(old);
        }
        return old == null ? null : old.value;
    }

    public void clear() {
        container.clear();
        numberContainer.clear();
        tail = null;
        head = null;
        preIndex = 0L;
    }

    public ScanResult scan(Long cursor, long count, Optional<String> pattern) {
        Optional<RPattern> p = pattern.map(RPattern::compile);
        Node<T> node = findScanBegin(cursor);
        ScanResult result = new ScanResult();
        if (node == null) {
            result.setNextCursor(0L);
            return result;
        }
        while (node != null && count-- > 0) {
            boolean matches = !p.isPresent() || p.get().matches(node.key.str());
            if (matches) result.add(node.key);
            node = node.next;
        }
        long nextCursor = node == null ? 0L : node.idx;
        result.setNextCursor(nextCursor);
        return result;
    }

    private Node<T> findScanBegin(long cursor) {
        if (cursor == 0) {
            return head;
        }
        Node<T> t = null;
        while (t == null && cursor <= preIndex) {
            t = numberContainer.get(cursor++);
        }
        return t;
    }

    public static class ScanResult {
        private Long nextCursor;
        private final List<Key> keyNames = new ArrayList<>();

        private void add(Key k) {
            keyNames.add(k);
        }

        void setNextCursor(Long c) {
            this.nextCursor = c;
        }

        public Long getNextCursor() {
            return nextCursor;
        }

        public List<Key> getKeyNames() {
            return keyNames;
        }
    }


    public static class Node<T> {
        final Key key;
        final T value;
        final long idx;
        Node<T> pre;
        Node<T> next;

        private Node(Key key, T value, long idx) {
            this.key = key;
            this.value = value;
            this.idx = idx;
        }

        public long getIdx() {
            return idx;
        }

        public Node<T> getPre() {
            return pre;
        }

        public Node<T> getNext() {
            return next;
        }

        public T getValue() {
            return value;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("Node{");
            sb.append("value=").append(value);
            sb.append(", idx=").append(idx);
            sb.append(", next=").append(next == null ? "null" : next.value);
            sb.append('}');
            return sb.toString();
        }
    }
}
