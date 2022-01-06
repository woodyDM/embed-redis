package cn.deepmax.redis.core.module;

import cn.deepmax.redis.core.Sized;
import cn.deepmax.redis.utils.Range;
import cn.deepmax.redis.utils.Tuple;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @param <S>
 * @param <T>
 */
public class ZSet<S extends Comparable<S>, T extends Comparable<T>> implements Sized {

    static final int ZSKIPLIST_MAXLEVEL = 32;
    static final double ZSKIPLIST_P = 0.25D;
    final ZSkipList<S, T> zsl;
    final ScanMap<T, S> dict;

    public ZSet() {
        this.zsl = ZSkipList.newInstance();
        this.dict = new ScanMap<>();
    }

    @Override
    public long size() {
        if (zsl.length != dict.size()) {
            throw new IllegalStateException("invalid zset, check code");
        }
        return dict.size();
    }

    public int add(List<Pair<S, T>> values) {
        int c = 0;
        for (Pair<S, T> t : values) {
            S old = dict.set(t.ele, t.score);
            if (old == null) {
                c++;
                zsl.insert(t.score, t.ele);
            } else {
                zsl.updateScore(t.ele, old, t.score);
            }
        }
        return c;
    }

    /**
     * indexRange
     *
     * @param start
     * @param end
     * @param rev
     * @return
     */
    public List<Pair<S, T>> indexRange(int start, int end, boolean rev) {
        start = tranStart(start);
        end = tranEnd(end);
        if (start > end) {
            return Collections.emptyList();
        }
        int rangeLength = end - start + 1;
        ZSkipListNode<S, T> cur;
        if (rev) {
            cur = start == 0 ? zsl.tail : zsl.getByRank(zsl.length - start);
        } else {
            cur = start == 0 ? zsl.header.next() : zsl.getByRank(start + 1);
        }
        List<Pair<S, T>> result = new ArrayList<>();
        while (rangeLength-- > 0) {
            result.add(new Pair<>(cur.score, cur.ele));
            cur = cur.next(rev);
        }
        return result;
    }

    /**
     * ScoreRange
     *
     * @param range
     * @param rev
     * @param optLimit
     * @return
     */
    public List<Pair<S, T>> scoreRange(Range<S> range, boolean rev, Optional<Tuple<Long, Long>> optLimit) {
        ZSkipListNode<S, T> cur;
        if (rev) {
            cur = zsl.zslLastInRange(range);
        } else {
            cur = zsl.zslFirstInRange(range);
        }
        if (cur == null) {
            return Collections.emptyList();
        }
        long offset = optLimit.map(t -> t.a).orElse(0L);
        long limit = optLimit.filter(t -> t.b > 0).map(t -> t.b).orElse(zsl.length);
        //skip offset and then get at most limit elements;
        while (cur != null && offset-- > 0) {
            cur = cur.next(rev);
        }
        List<Pair<S, T>> result = new ArrayList<>();
        while (cur != null && limit-- > 0) {
            //check range
            if (rev) {
                if (!cur.scoreGreaterOrEqualThanMinOf(range)) break;
            } else {
                if (!cur.scoreLessOrEqualThanMaxOf(range)) break;
            }
            result.add(new Pair<>(cur.score, cur.ele));
            cur = cur.next(rev);
        }
        return result;
    }

    /**
     * lexRange
     *
     * @param range
     * @param rev
     * @param optLimit
     * @return
     */
    public List<Pair<S, T>> lexRange(Range<T> range, boolean rev, Optional<Tuple<Long, Long>> optLimit) {
        ZSkipListNode<S, T> cur;
        if (rev) {
            cur = zsl.zslLastLexInRange(range);
        } else {
            cur = zsl.zslFirstLexInRange(range);
        }
        if (cur == null) {
            return Collections.emptyList();
        }
        long offset = optLimit.map(t -> t.a).orElse(0L);
        long limit = optLimit.filter(t -> t.b > 0).map(t -> t.b).orElse(zsl.length);
        //skip offset and then get at most limit elements;
        while (cur != null && offset-- > 0) {
            cur = cur.next(rev);
        }
        List<Pair<S, T>> result = new ArrayList<>();
        while (cur != null && limit-- > 0) {
            //check range
            if (rev) {
                if (!cur.eleGreaterOrEqualThanMinOf(range)) break;
            } else {
                if (!cur.eleLessOrEqualThanMaxOf(range)) break;
            }
            result.add(new Pair<>(cur.score, cur.ele));
            cur = cur.next(rev);
        }
        return result;
    }

    public int rank(T member) {
        S score = dict.get(member);
        if (score == null) {
            return -1;
        }
        // 1-based
        long rank = zsl.getRank(member, score);
        return (int) (rank - 1);
    }

    /**
     * 倒序
     *
     * @param member
     * @return
     */
    public int revRank(T member) {
        S score = dict.get(member);
        if (score == null) {
            return -1;
        }
        // 1-based
        long rank = zsl.getRank(member, score);
        if (rank == 0) {
            return -1;
        }
        return (int) (zsl.length - rank);
    }

    public int removeByRank(int min, int max) {
        min = tranStart(min);
        max = tranEnd(max);
        if (min > max) {
            return 0;
        }
        List<T> ele = zsl.zslDeleteRangeByRank(min + 1, max + 1);
        ele.forEach(dict::delete);
        return ele.size();
    }

    public int removeByScore(Range<S> range) {
        List<T> ele = zsl.zslDeleteRangeByRange(range);
        ele.forEach(dict::delete);
        return ele.size();
    }

    public int removeByLex(Range<T> range) {
        List<T> ele = zsl.zslDeleteRangeByLex(range);
        ele.forEach(dict::delete);
        return ele.size();
    }

    public int remove(T member) {
        ScanMap.Node<T, S> s = dict.delete(member);
        if (s == null) return 0;
        zsl.delete(s.value, member);
        return 1;
    }

    public S score(T key) {
        return dict.get(key);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static class ZSkipList<S extends Comparable<S>, T extends Comparable<T>> implements Sized {
        ZSkipListNode<S, T> header;
        ZSkipListNode<S, T> tail;
        long length;
        int level;  //levelRange: [0,level)

        public static <S extends Comparable<S>, T extends Comparable<T>> ZSkipList<S, T> newInstance() {
            ZSkipList<S, T> t = new ZSkipList<>();
            t.length = 0;
            t.level = 1;
            t.header = zslCreateNode(ZSKIPLIST_MAXLEVEL, null, null);
            return t;
        }
        
        static <S extends Comparable<S>, T extends Comparable<T>> ZSkipListNode<S, T> zslCreateNode(int levelNumber, T ele, S score) {
            ZSkipListNode<S, T> node = new ZSkipListNode<>(ele, score);
            node.level = new Level[levelNumber];
            for (int i = 0; i < levelNumber; i++) {
                node.level[i] = new Level<>();
            }
            return node;
        }

        @Override
        public long size() {
            return length;
        }

        /**
         * find
         *
         * @param idx start at 0   [0,length)
         * @return
         */
        public ZSkipListNode<S, T> getByIndex(int idx) {
            idx = tranStart(idx);
            return getByRank(idx + 1);
        }

        /**
         * find
         *
         * @param rank start at 1   [1,length]
         * @return
         */
        public ZSkipListNode<S, T> getByRank(long rank) {
            if (rank > length) {
                return null;
            }
            int c = 0;
            ZSkipListNode<S, T> cur = this.header;
            for (int lv = level - 1; lv >= 0; lv--) {
                while (cur.level[lv].forward != null && cur.level[lv].span + c <= rank) {
                    c += cur.level[lv].span;
                    cur = cur.level[lv].forward;
                }
                if (c == rank) {
                    return cur;
                }
            }
            return null;
        }

        /**
         * range中的第一个：比开始的第一个
         *
         * @param r
         * @return
         */
        public ZSkipListNode<S, T> zslFirstInRange(Range<S> r) {
            if (!inRange(r)) {
                return null;
            }
            ZSkipListNode<S, T> x = header;
            for (int i = level - 1; i >= 0; i--) {
                /* Go forward while *OUT* of range. */
                while (x.level[i].forward != null &&
                        !x.level[i].forward.scoreGreaterOrEqualThanMinOf(r)) {
                    x = x.level[i].forward;
                }
            }
            x = x.level[0].forward;
            /* Check if score <= max. */
            if (!x.scoreLessOrEqualThanMaxOf(r)) return null;
            return x;
        }

        /**
         * range 中的最后一个
         *
         * @param r
         * @return
         */
        public ZSkipListNode<S, T> zslLastInRange(Range<S> r) {
            if (!inRange(r)) {
                return null;
            }
            ZSkipListNode<S, T> x = header;
            for (int i = level - 1; i >= 0; i--) {
                /* Go forward while *IN* range. */
                while (x.level[i].forward != null &&
                        x.level[i].forward.scoreLessOrEqualThanMaxOf(r)) {
                    x = x.level[i].forward;
                }
            }
            /* Check if score >= min. */
            if (!x.scoreGreaterOrEqualThanMinOf(r)) return null;
            return x;
        }

        /**
         * lexRange中的第一个：比开始的第一个
         *
         * @param r
         * @return
         */
        public ZSkipListNode<S, T> zslFirstLexInRange(Range<T> r) {
            if (!lexInRange(r)) {
                return null;
            }
            ZSkipListNode<S, T> x = header;
            for (int i = level - 1; i >= 0; i--) {
                /* Go forward while *OUT* of range. */
                while (x.level[i].forward != null &&
                        !x.level[i].forward.eleGreaterOrEqualThanMinOf(r)) {
                    x = x.level[i].forward;
                }
            }
            x = x.level[0].forward;
            /* Check if ele <= max. */
            if (!x.eleLessOrEqualThanMaxOf(r)) return null;
            return x;
        }

        /**
         * range 中的最后一个
         *
         * @param r
         * @return
         */
        public ZSkipListNode<S, T> zslLastLexInRange(Range<T> r) {
            if (!lexInRange(r)) {
                return null;
            }
            ZSkipListNode<S, T> x = header;
            for (int i = level - 1; i >= 0; i--) {
                /* Go forward while *IN* range. */
                while (x.level[i].forward != null &&
                        x.level[i].forward.eleLessOrEqualThanMaxOf(r)) {
                    x = x.level[i].forward;
                }
            }
            /* Check if ele >= min. */
            if (!x.eleGreaterOrEqualThanMinOf(r)) return null;
            return x;
        }

        private boolean inRange(Range<S> r) {
            if (r.getStart().compareTo(r.getEnd()) > 0
                    || (r.getStart().compareTo(r.getEnd()) == 0) && (r.isEndOpen() || r.isStartOpen())) {
                return false;
            }
            if (tail == null || !tail.scoreGreaterOrEqualThanMinOf(r)) {
                return false;
            }
            ZSkipListNode<S, T> first = header.level[0].forward;
            if (first == null || !first.scoreLessOrEqualThanMaxOf(r)) {
                return false;
            }
            return true;
        }

        private boolean lexInRange(Range<T> r) {
            if (r.getStart().compareTo(r.getEnd()) > 0
                    || (r.getStart().compareTo(r.getEnd()) == 0) && (r.isEndOpen() || r.isStartOpen())) {
                return false;
            }
            if (tail == null || !tail.eleGreaterOrEqualThanMinOf(r)) {
                return false;
            }
            ZSkipListNode<S, T> first = header.level[0].forward;
            if (first == null || !first.eleLessOrEqualThanMaxOf(r)) {
                return false;
            }
            return true;
        }

        //ele and old score must exist!
        public ZSkipListNode<S, T> updateScore(T ele, S old, S newScore) {
            Objects.requireNonNull(old);
            Objects.requireNonNull(newScore);
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            ZSkipListNode<S, T> x = header;
            ZSkipListNode<S, T> dummy = new ZSkipListNode<>(ele, old);
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
                return insert(newScore, ele);
            }
        }

        private IllegalStateException notFoundError(T ele, S old) {
            return new IllegalStateException("Can't found score " + old + " with ele " + ele);
        }

        /**
         * @param ele
         * @param score
         * @return 1-based rank  or 0 if not found
         */
        public long getRank(T ele, S score) {
            ZSkipListNode<S, T> dummy = new ZSkipListNode<>(ele, score);
            ZSkipListNode<S, T> x = header;
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

        /**
         * @param start 1-based include
         * @param end   1-based include
         * @return removed node values
         */
        public List<T> zslDeleteRangeByRank(long start, long end) {
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            ZSkipListNode<S, T> x = header;
            long c = 0;
            for (int i = level - 1; i >= 0; i--) {
                while (x.level[i].forward != null && x.level[i].span + c < start) {
                    c += x.level[i].span;
                    x = x.level[i].forward;
                }
                update[i] = x;
            }
            //should delete update.next;
            List<T> list = new ArrayList<>();
            x = x.next();
            c++;
            while (x != null && c <= end) {
                list.add(x.ele);
                deleteNode(x, update);
                c++;
                x = x.next();
            }
            return list;
        }

        /**
         * @param range
         * @return removed node values
         */
        public List<T> zslDeleteRangeByRange(Range<S> range) {
            if (!inRange(range)) {
                return Collections.emptyList();
            }
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            ZSkipListNode<S, T> x = header;
            for (int i = level - 1; i >= 0; i--) {
                while (x.level[i].forward != null && !x.level[i].forward.scoreGreaterOrEqualThanMinOf(range)) {
                    x = x.level[i].forward;
                }
                update[i] = x;
            }
            //should delete update.next;
            List<T> list = new ArrayList<>();
            x = x.next();
            while (x != null && x.scoreLessOrEqualThanMaxOf(range)) {
                list.add(x.ele);
                deleteNode(x, update);
                x = x.next();
            }
            return list;
        }

        /**
         * @param range
         * @return removed node values
         */
        public List<T> zslDeleteRangeByLex(Range<T> range) {
            if (!lexInRange(range)) {
                return Collections.emptyList();
            }
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            ZSkipListNode<S, T> x = header;
            for (int i = level - 1; i >= 0; i--) {
                while (x.level[i].forward != null && !x.level[i].forward.eleGreaterOrEqualThanMinOf(range)) {
                    x = x.level[i].forward;
                }
                update[i] = x;
            }
            //should delete update.next;
            List<T> list = new ArrayList<>();
            x = x.next();
            while (x != null && x.eleLessOrEqualThanMaxOf(range)) {
                list.add(x.ele);
                deleteNode(x, update);
                x = x.next();
            }
            return list;
        }

        public ZSkipListNode<S, T> delete(S score, T ele) {
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            ZSkipListNode<S, T> x = header;
            ZSkipListNode<S, T> dummy = new ZSkipListNode<>(ele, score);
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

        private void deleteNode(ZSkipListNode<S, T> node, ZSkipListNode[] update) {
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

        public ZSkipListNode<S, T> insert(S score, T ele) {
            ZSkipListNode[] update = new ZSkipListNode[ZSKIPLIST_MAXLEVEL];
            int[] rank = new int[ZSKIPLIST_MAXLEVEL];
            int i;
            ZSkipListNode<S, T> x = header;
            int thisLevel = zslRandomLevel();
            ZSkipListNode<S, T> theNode = zslCreateNode(thisLevel, ele, score);
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

    static class ZSkipListNode<S extends Comparable<S>, T extends Comparable<T>> implements Comparable<ZSkipListNode<S, T>> {
        T ele;
        S score;
        ZSkipListNode<S, T> backward;
        Level<S, T>[] level;

        ZSkipListNode(T ele, S score) {
            this.ele = ele;
            this.score = score;
        }

        ZSkipListNode<S, T> next() {
            return level[0].forward;
        }


        ZSkipListNode<S, T> next(boolean reversed) {
            if (reversed) {
                return backward;
            } else {
                return level[0].forward;
            }
        }

        public boolean scoreGreaterOrEqualThanMinOf(Range<S> s) {
            if (s.isStartOpen()) {
                return score.compareTo(s.getStart()) > 0;
            } else {
                return score.compareTo(s.getStart()) >= 0;
            }
        }

        public boolean scoreLessOrEqualThanMaxOf(Range<S> s) {
            if (s.isEndOpen()) {
                return score.compareTo(s.getEnd()) < 0;
            } else {
                return score.compareTo(s.getEnd()) <= 0;
            }
        }

        public boolean eleGreaterOrEqualThanMinOf(Range<T> s) {
            if (s.isStartOpen()) {
                return ele.compareTo(s.getStart()) > 0;
            } else {
                return ele.compareTo(s.getStart()) >= 0;
            }
        }

        public boolean eleLessOrEqualThanMaxOf(Range<T> s) {
            if (s.isEndOpen()) {
                return ele.compareTo(s.getEnd()) < 0;
            } else {
                return ele.compareTo(s.getEnd()) <= 0;
            }
        }

        public boolean lessThan(ZSkipListNode<S, T> that) {
            return compareTo(that) < 0;
        }

        public boolean lessOrEqThan(ZSkipListNode<S, T> that) {
            int i = compareTo(that);
            return i < 0 || i == 0;
        }

        @Override
        public int compareTo(ZSkipListNode<S, T> that) {
            return Comparator.comparing((ZSkipListNode<S, T> n) -> n.score)
                    .thenComparing((ZSkipListNode<S, T> n) -> n.ele).compare(this, that);
        }
    }

    static class Level<S extends Comparable<S>, T extends Comparable<T>> {
        ZSkipListNode<S, T> forward;
        long span;
    }

    public static class Pair<S extends Comparable<S>, T extends Comparable<T>> {
        public final S score;
        public final T ele;

        public Pair(S score, T ele) {
            this.score = score;
            this.ele = ele;
        }
    }

}
