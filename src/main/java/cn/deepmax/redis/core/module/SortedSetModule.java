package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import cn.deepmax.redis.utils.Range;
import cn.deepmax.redis.utils.Tuple;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class SortedSetModule extends BaseModule {
    public SortedSetModule() {
        super("sortedSet");
        register(new ZAdd());
        register(new ZScore());
        register(new ZMScore());
        register(new ZRange());
        register(new ZRevRange());
        register(new ZRangeByScore(false, "zrangebyscore"));
        register(new ZRangeByScore(true, "zrevrangebyscore"));
        register(new ZRangeByLex(false, "zrangebylex"));
        register(new ZRangeByLex(true, "zrevrangebylex"));
        register(new ZRangeStore());
        register(new ZRank());
        register(new ZRevRank());
        register(new ZRemRangeByRank());
        register(new ZRemRangeByScore());
        register(new ZRemRangeByLex());
        register(new ZRem());
    }

    /**
     * todo
     * ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
     */
    public static class ZAdd extends ArgsCommand.FourWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            //scan from tail
            int idx = msg.children().size() - 2;
            List<ZSet.Pair<Double, Key>> values = new ArrayList<>();
            Optional<Double> v;
            while (idx >= 2 && (v = NumberUtils.parseDoubleO(msg.getAt(idx).str())).isPresent()) {
                values.add(new ZSet.Pair<>(v.get(), new Key(msg.getAt(idx + 1).bytes())));
                idx -= 2;
            }
            if (values.isEmpty()) {
                return Constants.ERR_SYNTAX;
            }
            //todo check from [2,idx+1] flags  
            SortedSet set = get(key);
            if (set == null) {
                set = new SortedSet(engine.timeProvider());
                engine.getDb(client).set(client, key, set);
            }
            //todo return values
            int updated = set.add(values);
            return new IntegerRedisMessage(updated);
        }
    }

    public static class ZRem extends ArgsCommand.ThreeWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> members = genKeys(msg.children(), 2);
            SortedSet set = get(key);
            if (set == null) {
                return Constants.INT_ZERO;
            }
            int c = 0;
            for (Key member : members) {
                c += set.remove(member);
            }
            deleteSetIfNeed(key, engine, client);
            return new IntegerRedisMessage(c);
        }
    }

    public static class ZScore extends ArgsCommand.ThreeExWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] member = msg.getAt(2).bytes();
            SortedSet set = get(key);
            if (set == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            Double d = set.score(new Key(member));
            if (d == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            return FullBulkValueRedisMessage.ofString(NumberUtils.formatDouble(d));
        }
    }

    public static class ZMScore extends ArgsCommand.ThreeWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> members = genKeys(msg.children(), 2);
            SortedSet set = get(key);
            if (set == null) {
                return ListRedisMessage.empty();
            }
            List<RedisMessage> list = members.stream().map(k -> {
                Double score = set.score(k);
                if (score == null) {
                    return FullBulkValueRedisMessage.NULL_INSTANCE;
                } else {
                    return FullBulkValueRedisMessage.ofString(NumberUtils.formatDouble(score));
                }
            }).collect(Collectors.toList());
            return new ListRedisMessage(list);
        }
    }


    public static class ZRange extends BaseRange {
        public ZRange() {
            super(4);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] keys = msg.getAt(1).bytes();
            byte[] min = msg.getAt(2).bytes();
            byte[] max = msg.getAt(3).bytes();
            boolean withscores = ArgParser.parseFlag(msg, "WITHSCORES", 4);
            boolean byScore = ArgParser.parseFlag(msg, "BYSCORE", 4);
            boolean byLex = ArgParser.parseFlag(msg, "BYLEX", 4);
            boolean reverse = ArgParser.parseFlag(msg, "REV", 4);
            Optional<Tuple<Long, Long>> limit = ArgParser.parseLongArgTwo(msg, "LIMIT", 4, msg.children().size());
            RangeType type = RangeType.of(byScore, byLex);
            //checks
            if (limit.isPresent() && type == RangeType.INDEX) {
                return new ErrorRedisMessage("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
            }
            if (withscores && type == RangeType.LEX) {
                return new ErrorRedisMessage("syntax error, WITHSCORES not supported in combination with BYLEX");
            }
            return genericZRange(keys, type, min, max, reverse, withscores, limit);
        }

        @Override
        public Optional<ErrorRedisMessage> preCheckLength(RedisMessage type) {
            if (cast(type).children().size() > 10) {
                return Optional.of(Constants.ERR_SYNTAX);
            }
            return super.preCheckLength(type);
        }
    }

    /**
     * ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
     */
    public static class ZRangeByScore extends BaseRange {
        private final boolean rev;
        private final String name;

        public ZRangeByScore(boolean rev, String name) {
            super(4, 5, 7, 8);
            this.rev = rev;
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] keys = msg.getAt(1).bytes();
            byte[] min = msg.getAt(2).bytes();
            byte[] max = msg.getAt(3).bytes();
            boolean withscores = ArgParser.parseFlag(msg, "WITHSCORES", 4);
            Optional<Tuple<Long, Long>> optLimit = ArgParser.parseLongArgTwo(msg, "LIMIT", 4, msg.children().size());
            return genericZRange(keys, RangeType.SCORE, min, max, rev, withscores, optLimit);
        }
    }

    /**
     * ZRANGEBYLEX key min max [LIMIT offset count]
     * ZREVRANGEBYLEX key max min [LIMIT offset count]
     */
    public static class ZRangeByLex extends BaseRange {
        private final boolean rev;
        private final String name;

        public ZRangeByLex(boolean rev, String name) {
            super(4, 7);
            this.rev = rev;
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] keys = msg.getAt(1).bytes();
            byte[] min = msg.getAt(2).bytes();
            byte[] max = msg.getAt(3).bytes();
            Optional<Tuple<Long, Long>> optLimit = ArgParser.parseLongArgTwo(msg, "LIMIT", 4, msg.children().size());
            return genericZRange(keys, RangeType.LEX, min, max, rev, false, optLimit);
        }
    }

    public static class ZRevRange extends BaseRange {
        public ZRevRange() {
            super(4, 5);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] keys = msg.getAt(1).bytes();
            byte[] start = msg.getAt(2).bytes();
            byte[] stop = msg.getAt(3).bytes();
            boolean withscores = ArgParser.parseFlag(msg, "WITHSCORES", 4);
            return genericZRange(keys, RangeType.INDEX, start, stop, true, withscores, Optional.empty());
        }
    }

    public static class ZRangeStore extends BaseRange {
        public ZRangeStore() {
            super(5, 6, 7, 8, 9, 10);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] dest = msg.getAt(1).bytes();
            byte[] src = msg.getAt(2).bytes();
            byte[] min = msg.getAt(3).bytes();
            byte[] max = msg.getAt(4).bytes();
            boolean withscores = ArgParser.parseFlag(msg, "WITHSCORES", 5);
            boolean byScore = ArgParser.parseFlag(msg, "BYSCORE", 5);
            boolean byLex = ArgParser.parseFlag(msg, "BYLEX", 5);
            boolean reverse = ArgParser.parseFlag(msg, "REV", 5);
            Optional<Tuple<Long, Long>> limit = ArgParser.parseLongArgTwo(msg, "LIMIT", 5, msg.children().size());
            RangeType type = RangeType.of(byScore, byLex);
            //checks
            if (limit.isPresent() && type == RangeType.INDEX) {
                return new ErrorRedisMessage("syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
            }
            if (withscores && type == RangeType.LEX) {
                return new ErrorRedisMessage("syntax error, WITHSCORES not supported in combination with BYLEX");
            }
            return genericZRangeStore(src, dest, type, min, max, reverse, limit);
        }
    }

    abstract static class BaseRange extends ArgsCommand<SortedSet> {
        public BaseRange(int limit) {
            super(limit);
        }

        public BaseRange(int... valid) {
            super(valid);
        }

        /**
         * @param key
         * @param type
         * @param startB
         * @param endB
         * @param rev
         * @param optLimit
         * @return null if key not found!
         */
        protected List<ZSet.Pair<Double, Key>> genericZRange0(byte[] key, RangeType type, byte[] startB, byte[] endB, boolean rev, Optional<Tuple<Long, Long>> optLimit) {

            if (rev && (type == RangeType.SCORE || type == RangeType.LEX)) {
                /* Range is given as [max,min] */
                byte[] tmp = startB;
                startB = endB;
                endB = tmp;
            }
            String start = new String(startB, StandardCharsets.UTF_8);
            String end = new String(endB, StandardCharsets.UTF_8);
            SortedSet set = get(key);

            if (type == RangeType.INDEX) {
                int min = NumberUtils.parse(start).intValue();
                int max = NumberUtils.parse(end).intValue();
                if (set == null) {
                    return null;
                }
                return set.indexRange(min, max, rev);
            } else if (type == RangeType.SCORE) {
                Range<Double> r = NumberUtils.parseScoreRange(start, end);
                if (set == null) {
                    return null;
                }
                return set.scoreRange(r, rev, optLimit);
            } else if (type == RangeType.LEX) {
                Range<Key> r = NumberUtils.parseLexRange(startB, endB);
                if (set == null) {
                    return null;
                }
                return set.lexRange(r, rev, optLimit);
            } else {
                throw new Error();
            }
        }

        protected RedisMessage genericZRange(byte[] key, RangeType type, byte[] startB, byte[] endB, boolean rev,
                                             boolean withScores, Optional<Tuple<Long, Long>> optLimit) {
            List<ZSet.Pair<Double, Key>> range = this.genericZRange0(key, type, startB, endB, rev, optLimit);
            return range == null ? ListRedisMessage.empty() : transPair(range, withScores);
        }

        protected RedisMessage genericZRangeStore(byte[] src, byte[] dest, RangeType type, byte[] startB, byte[] endB, boolean rev,
                                                  Optional<Tuple<Long, Long>> optLimit) {
            List<ZSet.Pair<Double, Key>> range = this.genericZRange0(src, type, startB, endB, rev, optLimit);
            engine.getDb(client).del(client, dest);
            //del dest key and add if need
            if (range != null && !range.isEmpty()) {
                SortedSet set = new SortedSet(engine.timeProvider());
                int i = set.add(range);
                engine.getDb(client).set(client, dest, set);
                return new IntegerRedisMessage(i);
            } else {
                return Constants.INT_ZERO;
            }
        }
    }

    enum RangeType {
        INDEX, SCORE, LEX;

        static RangeType of(boolean byScore, boolean byLex) {
            if (byScore && byLex) {
                throw new RedisServerException(Constants.ERR_SYNTAX);
            } else if (byScore) {
                return SCORE;
            } else if (byLex) {
                return LEX;
            } else {
                return INDEX;
            }
        }
    }

    static RedisMessage transPair(List<ZSet.Pair<Double, Key>> list, boolean withScores) {
        if (list == null) {
            return ListRedisMessage.empty();
        }
        List<RedisMessage> msgList = new ArrayList<>();
        for (ZSet.Pair<Double, Key> p : list) {
            msgList.add(FullBulkValueRedisMessage.ofString(p.ele.getContent()));
            if (withScores) msgList.add(FullBulkValueRedisMessage.ofString(NumberUtils.formatDouble(p.score)));
        }
        return new ListRedisMessage(msgList);
    }

    public static class ZRank extends ArgsCommand.ThreeExWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] member = msg.getAt(2).bytes();
            SortedSet set = get(key);
            if (set == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            int r = set.rank(new Key(member));
            if (r == -1) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            return new IntegerRedisMessage(r);
        }
    }

    public static class ZRevRank extends ArgsCommand.ThreeExWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] member = msg.getAt(2).bytes();
            SortedSet set = get(key);
            if (set == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            int r = set.revRank(new Key(member));
            if (r == -1) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            return new IntegerRedisMessage(r);
        }
    }

    public static class ZRemRangeByRank extends ArgsCommand.FourExWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long min = msg.getAt(2).val();
            Long max = msg.getAt(3).val();
            SortedSet set = get(key);
            if (set == null) {
                return Constants.INT_ZERO;
            }
            int r = set.removeByRank(min.intValue(), max.intValue());
            deleteSetIfNeed(key, engine, client);
            return new IntegerRedisMessage(r);
        }
    }

    public static class ZRemRangeByScore extends ArgsCommand.FourExWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            String min = msg.getAt(2).str();
            String max = msg.getAt(3).str();
            SortedSet set = get(key);
            if (set == null) {
                return Constants.INT_ZERO;
            }
            Range<Double> range = NumberUtils.parseScoreRange(min, max);
            int r = set.removeByScore(range);
            deleteSetIfNeed(key, engine, client);
            return new IntegerRedisMessage(r);
        }
    }

    public static class ZRemRangeByLex extends ArgsCommand.FourExWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] min = msg.getAt(2).bytes();
            byte[] max = msg.getAt(3).bytes();
            SortedSet set = get(key);
            if (set == null) {
                return Constants.INT_ZERO;
            }
            Range<Key> range = NumberUtils.parseLexRange(min, max);
            int r = set.removeByLex(range);
            deleteSetIfNeed(key, engine, client);
            return new IntegerRedisMessage(r);
        }
    }

    /**
     * remove set which size = 0 or fire update event
     *
     * @param key
     * @param engine
     * @param client
     */
    static void deleteSetIfNeed(byte[] key, RedisEngine engine, Client client) {
        SortedSet afterSet = (SortedSet) engine.getDb(client).get(client, key);
        if (afterSet.size() == 0) {
            engine.getDb(client).del(client, key);
        } else {
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
        }
    }

}
