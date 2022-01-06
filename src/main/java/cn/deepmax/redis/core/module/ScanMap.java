package cn.deepmax.redis.core.module;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ScanMap<K, V> {
    private final Map<K, Node<K, V>> container = new HashMap<>();
    private final Map<Long, Node<K, V>> numberContainer = new HashMap<>();
    Node<K, V> tail;
    Node<K, V> head;
    private long preIndex = 0L;

    public V get(K key) {
        Node<K, V> n = container.get(key);
        return n == null ? null : n.value;
    }

    public Node<K, V> get(Long idx) {
        return numberContainer.get(idx);
    }

    public int size() {
        return container.size();
    }

    public Node<K, V> delete(K key) {
        Node<K, V> old = container.remove(key);
        if (old == null) {
            return null;
        }
        if (old == tail) {
            tail = old.pre;
        }
        removeOld(old);
        return old;
    }

    private void removeOld(Node<K, V> old) {
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


    public V set(K key, V t) {
        preIndex++;
        //append node to last
        Node<K, V> node = new Node<>(key, t, preIndex);
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
        Node<K, V> old = container.put(key, node);
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

    public ScanResult<K> scan(Long cursor, long count, Function<K, Boolean> matcherAction) {
        Node<K, V> node = findScanBegin(cursor);
        ScanResult<K> result = new ScanResult();
        if (node == null) {
            result.setNextCursor(0L);
            return result;
        }
        while (node != null && count-- > 0) {
            boolean matches = matcherAction.apply(node.key);
            if (matches) result.add(node.key);
            node = node.next;
        }
        long nextCursor = node == null ? 0L : node.idx;
        result.setNextCursor(nextCursor);
        return result;
    }

    private Node<K, V> findScanBegin(long cursor) {
        if (cursor == 0) {
            return head;
        }
        Node<K, V> t = null;
        while (t == null && cursor <= preIndex) {
            t = numberContainer.get(cursor++);
        }
        return t;
    }

    public static class ScanResult<K> {
        private Long nextCursor;
        private final List<K> keyNames = new ArrayList<>();

        private void add(K k) {
            keyNames.add(k);
        }

        void setNextCursor(Long c) {
            this.nextCursor = c;
        }

        public Long getNextCursor() {
            return nextCursor;
        }

        public List<K> getKeyNames() {
            return keyNames;
        }
    }

    public static class Node<K, V> {
        final K key;
        final V value;
        final long idx;
        Node<K, V> pre;
        Node<K, V> next;

        private Node(K key, V value, long idx) {
            this.key = key;
            this.value = value;
            this.idx = idx;
        }

        public long getIdx() {
            return idx;
        }

        public Node<K, V> getPre() {
            return pre;
        }

        public Node<K, V> getNext() {
            return next;
        }

        public V getValue() {
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
