package cn.deepmax.redis.core.support;

import cn.deepmax.redis.core.Key;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wudi
 * @date 2021/7/2
 */
public class NavMap<T> {
    private final Map<Key, Node<T>> container = new HashMap<>();
    private final Map<Integer, Node<T>> numberContainer = new HashMap<>();
    private Node<T> lastNode;
    private int index = 0;

    public static class Node<T> {
        Node<T> pre;
        Node<T> next;
        final T value;
        final Integer idx;

        private Node(T value, Integer idx) {
            this.value = value;
            this.idx = idx;
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

        public Integer getIdx() {
            return idx;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("Node{");
            sb.append("value=").append(value);
            sb.append(", idx=").append(idx);
            sb.append('}');
            return sb.toString();
        }
    }

    public T get(Key key) {
        Node<T> n = container.get(key);
        return n == null ? null : n.value;
    }

    public Node<T> get(Integer idx) {
        return numberContainer.get(idx);
    }

    public int size() {
        return container.size();
    }

    public void delete(Key key) {
        Node<T> old = container.remove(key);
        if (old == null) {
            return;
        }
        if (old == lastNode) {
            lastNode = old.pre;
        }
        removeOld(old);
    }

    void removeOld(Node<T> old) {
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

    public T set(Key key, T t) {
        index++;
        Node<T> node = new Node<>(t, index);
        node.pre = lastNode;

        numberContainer.put(index, node);
        Node<T> old = container.put(key, node);
        if (old != null) {
            removeOld(old);
        } else {
            if (lastNode != null) {
                lastNode.next = node;
            }
        }
        lastNode = node;
        return old == null ? null : old.value;
    }
}
