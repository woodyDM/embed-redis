package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
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
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class SortedSetModule extends BaseModule {
    public SortedSetModule() {
        super("sortedSet");
        register(new ZAdd());
        register(new ZRange());
        register(new ZRevRange());
        register(new ZRangeByScore(false, "zrangebyscore"));
        register(new ZRangeByScore(true, "zrevrangebyscore"));
        register(new ZRangeByLex(false, "zrangebylex"));
        register(new ZRangeByLex(true, "zrevrangebylex"));
    }

    /**
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

    abstract static class BaseRange extends ArgsCommand<SortedSet> {
        public BaseRange(int limit) {
            super(limit);
        }

        public BaseRange(int... valid) {
            super(valid);
        }

        protected RedisMessage genericZRange(byte[] key, RangeType type, byte[] startB, byte[] endB, boolean rev,
                                             boolean withScores, Optional<Tuple<Long, Long>> optLimit) {

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
                    return Constants.LIST_EMPTY;
                }
                List<ZSet.Pair<Double, Key>> range = set.indexRange(min, max, rev);
                return transPair(range, withScores);
            } else if (type == RangeType.SCORE) {
                Range<Double> r = NumberUtils.parseScoreRange(start, end);
                if (set == null) {
                    return Constants.LIST_EMPTY;
                }
                List<ZSet.Pair<Double, Key>> range = set.scoreRange(r, rev, optLimit);
                return transPair(range, withScores);
            } else if (type == RangeType.LEX) {
                Range<Key> r = NumberUtils.parseLexRange(startB, endB);
                if (set == null) {
                    return Constants.LIST_EMPTY;
                }
                List<ZSet.Pair<Double, Key>> range = set.lexRange(r, rev, optLimit);
                return transPair(range, withScores);
            } else {
                throw new Error();
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
            return Constants.LIST_EMPTY;
        }
        List<RedisMessage> msgList = new ArrayList<>();
        for (ZSet.Pair<Double, Key> p : list) {
            msgList.add(FullBulkValueRedisMessage.ofString(p.ele.getContent()));
            if (withScores) msgList.add(FullBulkValueRedisMessage.ofString(NumberUtils.formatDouble(p.score)));
        }
        return new ListRedisMessage(msgList);
    }
}
