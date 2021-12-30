package cn.deepmax.redis.core.module;

import cn.deepmax.redis.utils.Tuple;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 
 * @param <T> 
 * @param <V>
 */
public class ZSet<T extends Comparable<T>, V> {

    static final int ZSKIPLIST_MAXLEVEL = 32;
    static final double ZSKIPLIST_P = 0.25D;
    final ZSkipList<T> zsl;
    final Map<T, V> dict;

    public ZSet() {
        this.zsl = ZSkipList.newInstance();
        this.dict = new HashMap<>();
    }

    public void add(List<Tuple<T, V>> values) {
        
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static class ZSkipList<T extends Comparable<T>> {
        ZSkipListNode<T> header;
        ZSkipListNode<T> tail;
        long length;
        int level;  //level: [0,level)

        public static <T extends Comparable<T>> ZSkipList<T> newInstance() {
            ZSkipList<T> t = new ZSkipList<>();
            t.length = 0;
            t.level = 1;
            t.header = zslCreateNode(ZSKIPLIST_MAXLEVEL, null, BigDecimal.ZERO);
            return t;
        }

        static <T extends Comparable<T>> ZSkipListNode<T> zslCreateNode(int levelNumber, T ele, BigDecimal score) {
            ZSkipListNode<T> node = new ZSkipListNode<>(ele, score);
            node.level = new Level[levelNumber];
            for (int i = 0; i < levelNumber; i++) {
                node.level[i] = new Level<>();
            }
            return node;
        }

        //ele and old score must exist!
        public ZSkipListNode<T> updateScore(T ele, BigDecimal old, BigDecimal newScore) {
            Objects.requireNonNull(old);
            Objects.requireNonNull(newScore);
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            ZSkipListNode<T> x = header;
            ZSkipListNode<T> dummy = new ZSkipListNode<>(ele, old);
            for (int i = level - 1; i >= 0; i--) {
                while (x.level[i].forward != null && x.level[i].forward.lessThan(dummy)) {
                    x = x.level[i].forward;
                }
                update[i] = x;
            }
            x = x.level[0].forward;
            if (x == null || x.compareTo(dummy) != 0) {
                throw notFoundError(ele, old);
            }
            //try only update score
            if ((x.backward == null || x.backward.score.compareTo(newScore) < 0) &&
                    (x.level[0].forward == null || x.level[0].forward.score.compareTo(newScore) > 0)) {
                x.score = newScore;
                return x;
            } else {
                deleteNode(x, update);
                return insert(ele, newScore);
            }
        }

        private IllegalStateException notFoundError(T ele, BigDecimal old) {
            return new IllegalStateException("Can't found score " + old + " with ele " + ele);
        }

        /**
         * @param ele
         * @param score
         * @return 1-based rank  or 0 if not found
         */
        public long getRank(T ele, BigDecimal score) {
            ZSkipListNode<T> dummy = new ZSkipListNode<>(ele, score);
            ZSkipListNode<T> x = header;
            long c = 0L;
            for (int i = level - 1; i >= 0; i--) {
                while (x.level[i].forward != null && x.level[i].forward.lessOrEqThan(dummy)) {
                    c += x.level[i].span;
                    x = x.level[i].forward;
                }
                if (x.ele != null && x.compareTo(dummy) == 0) {
                    return c;
                }
            }
            return 0L;
        }

        public ZSkipListNode<T> delete(T ele, BigDecimal score) {
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            ZSkipListNode<T> x = header;
            ZSkipListNode<T> dummy = new ZSkipListNode<>(ele, score);
            for (int i = level - 1; i >= 0; i--) {
                while (x.level[i].forward != null && x.level[i].forward.lessThan(dummy)) {
                    x = x.level[i].forward;
                }
                update[i] = x;
            }
            x = update[0].level[0].forward;
            if (x != null && x.compareTo(dummy) == 0) {
                //found
                deleteNode(x, update);
                return x;
            }
            return null;
        }

        private void deleteNode(ZSkipListNode<T> node, ZSkipListNode[] update) {
            for (int i = 0; i < level; i++) {
                if (update[i].level[i].forward == node) {
                    update[i].level[i].span += (node.level[i].span - 1);
                    update[i].level[i].forward = node.level[i].forward;
                } else {
                    update[i].level[i].span--;
                }
            }
            if (node.level[0].forward != null) {
                node.level[0].forward.backward = node.backward;
            } else {
                tail = node.backward;
            }
            while (level > 1 && header.level[level - 1].forward == null) {
                level--;
            }
            length--;
        }

        public ZSkipListNode<T> insert(T ele, BigDecimal score) {
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            int[] rank = new int[ZSKIPLIST_MAXLEVEL];
            int i;
            ZSkipListNode<T> x = header;
            int thisLevel = zslRandomLevel();
            ZSkipListNode<T> theNode = zslCreateNode(thisLevel, ele, score);
            /* store rank that is crossed to reach the insert position */
            for (i = this.level - 1; i >= 0; i--) {
                rank[i] = (i == this.level - 1 ? 0 : rank[i + 1]);
                while (x.level[i].forward != null && x.level[i].forward.lessThan(theNode)) {
                    rank[i] += x.level[i].span;
                    x = x.level[i].forward;
                }
                update[i] = x;
            }
            //theNode should insert after update[i], update[i] （i<level)  is not null.
            if (thisLevel > level) {
                for (i = level; i < thisLevel; i++) {
                    rank[i] = 0;
                    update[i] = header;
                    update[i].level[i].span = length;
                }
                level = thisLevel;
            }
            //to insert theNode
            x = theNode;
            for (i = 0; i < thisLevel; i++) {
                x.level[i].forward = update[i].level[i].forward;
                update[i].level[i].forward = x;
                /* update span covered by update[i] as x is inserted here */
                x.level[i].span = update[i].level[i].span - (rank[0] - rank[i]);
                update[i].level[i].span = rank[0] - rank[i] + 1;
            }
            /* increment span for untouched levels */
            for (i = thisLevel; i < level; i++) {
                update[i].level[i].span++;
            }
            x.backward = (update[0] == header ? null : update[0]);
            if (x.level[0].forward != null) {
                x.level[0].forward.backward = x;
            } else {
                tail = x;
            }
            length++;
            return x;
        }

        static int zslRandomLevel() {
            int level = 1;
            while (ThreadLocalRandom.current().nextDouble() < ZSKIPLIST_P)
                level += 1;
            return Math.min(level, ZSKIPLIST_MAXLEVEL);
        }
    }

    static class ZSkipListNode<T extends Comparable<T>> implements Comparable<ZSkipListNode<T>> {
        T ele;
        BigDecimal score;
        ZSkipListNode<T> backward;
        Level<T>[] level;

        ZSkipListNode(T ele, BigDecimal score) {
            this.ele = ele;
            this.score = score;
        }

        public boolean lessThan(ZSkipListNode<T> that) {
            return compareTo(that) < 0;
        }

        public boolean lessOrEqThan(ZSkipListNode<T> that) {
            int i = compareTo(that);
            return i < 0 || i == 0;
        }

        @Override
        public int compareTo(ZSkipListNode<T> that) {
            return Comparator.comparing((ZSkipListNode<T> n) -> n.score)
                    .thenComparing((ZSkipListNode<T> n) -> n.ele).compare(this, that);
        }
    }

    static class Level<T extends Comparable<T>> {
        ZSkipListNode<T> forward;
        long span;
    }

}
