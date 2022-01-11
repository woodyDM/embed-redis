package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.SizedOperation;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static cn.deepmax.redis.core.RedisCommand.OK;

public class HashModule extends BaseModule {

    public HashModule() {
        super("hash");
        register("hset", new HSet(IntegerRedisMessage::new));
        register("hmset", new HSet(i -> OK));
        register(new HSetNx());
        register(new HStrLen());
        register(new HGet());
        register(new HMGet());
        register(new HGetAll());
        register("hkeys", new HIter(RHash::keys));
        register("hvals", new HIter(RHash::values));
        register(new HDel());
        register(new HLen());
        register(new HExists());
        register(new HIncrBy());
        register(new HIncrByFloat());
        register(new HRandField());
    }

    public static class HSet extends ArgsCommand.FourWith<RHash> {
        Function<Integer, RedisMessage> mapper;

        public HSet(Function<Integer, RedisMessage> mapper) {
            this.mapper = mapper;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<RHash.Pair> pairs = parsePair(msg, 2, msg.children().size());
            RHash hash = get(key);
            if (hash == null) {
                hash = new RHash(engine.timeProvider());
                engine.getDb(client).set(client, key, hash);
            }
            int i = hash.set(pairs);
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            return mapper.apply(i);
        }
    }

    public static class HSetNx extends ArgsCommand.FourExWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Key field = new Key(msg.getAt(2).bytes());
            Key value = new Key(msg.getAt(3).bytes());
            RHash hash = get(key);
            if (hash == null) {
                hash = new RHash(engine.timeProvider());
                hash.set(field, value);
                engine.getDb(client).set(client, key, hash);
                return Constants.INT_ONE;
            } else {
                Key v = hash.get(field);
                if (v == null) {
                    hash.set(field, value);
                    engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
                    return Constants.INT_ONE;
                } else {
                    return Constants.INT_ZERO;
                }
            }
        }
    }

    public static class HStrLen extends ArgsCommand.ThreeExWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Key field = new Key(msg.getAt(2).bytes());
            RHash hash = get(key);
            if (hash == null) {
                return Constants.INT_ZERO;
            } else {
                Key v = hash.get(field);
                if (v == null) {
                    return Constants.INT_ZERO;
                } else {
                    return new IntegerRedisMessage(v.getContent().length);
                }
            }
        }
    }

    public static class HRandField extends ArgsCommand<RHash> {
        public HRandField() {
            super(2, 3, 4);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long count = 1L;
            boolean hasCount = false;
            boolean withValues = false;
            int size = msg.children().size();
            if (size >= 3) {
                hasCount = true;
                count = msg.getAt(2).val();
            }
            if (size == 4) {
                String str = msg.getAt(3).str();
                if (!("WITHVALUES".equalsIgnoreCase(str))) {
                    return Constants.ERR_SYNTAX;
                }
                withValues = true;
            }
            if (count == 0) {
                return Constants.ERR_SYNTAX;
            }
            RHash hash = get(key);
            if (hash == null) {
                return hasCount ? ListRedisMessage.empty() : FullBulkValueRedisMessage.NULL_INSTANCE;
            }
            List<RHash.Pair> list = hash.randField(count);
            if (hasCount) {
                return transPair(list, withValues);
            } else {
                return FullBulkValueRedisMessage.ofString(list.get(0).field.getContent());
            }
        }
    }

    public static class HGet extends ArgsCommand.ThreeExWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] field = msg.getAt(2).bytes();
            RHash hash = get(key);
            return hget(hash, new Key(field));
        }
    }

    public static class HMGet extends ArgsCommand.ThreeWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> keys = genKeys(msg.children(), 2);
            RHash hash = get(key);
            List<RedisMessage> l = keys.stream().map(k -> hget(hash, k)).collect(Collectors.toList());
            return new ListRedisMessage(l);
        }
    }

    public static class HGetAll extends ArgsCommand.TwoExWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RHash hash = get(key);
            if (hash == null) {
                return ListRedisMessage.empty();
            }
            List<RHash.Pair> list = hash.getAll();
            return transPair(list, true);
        }
    }

    public static class HIter extends ArgsCommand.TwoExWith<RHash> {

        private Function<RHash, List<Key>> action;

        public HIter(Function<RHash, List<Key>> action) {

            this.action = action;
        }


        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RHash hash = get(key);
            if (hash == null) {
                return ListRedisMessage.empty();
            }
            List<Key> list = action.apply(hash);
            return ListRedisMessage.wrapKeys(list);
        }
    }

    public static class HDel extends ArgsCommand.ThreeWith<RHash> implements SizedOperation {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> fields = genKeys(msg.children(), 2);
            RHash hash = get(key);
            if (hash == null) {
                return Constants.INT_ZERO;
            }
            int eff = hash.del(fields);
            deleteEleIfNeed(key, engine, client);
            return new IntegerRedisMessage(eff);
        }
    }

    public static class HLen extends ArgsCommand.TwoExWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RHash hash = get(key);
            if (hash == null) {
                return Constants.INT_ZERO;
            }
            return new IntegerRedisMessage(hash.size());
        }
    }

    public static class HExists extends ArgsCommand.ThreeExWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] field = msg.getAt(2).bytes();
            RHash hash = get(key);
            if (hash == null) {
                return Constants.INT_ZERO;
            }
            Key v = hash.get(new Key(field));
            if (v == null) {
                return Constants.INT_ZERO;
            }
            return Constants.INT_ONE;
        }
    }

    public static class HIncrBy extends ArgsCommand.FourExWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Key field = new Key(msg.getAt(2).bytes());
            Long incr = msg.getAt(3).val();
            RHash hash = get(key);
            if (hash == null) {
                hash = new RHash(engine.timeProvider());
                hash.set(field, new Key(incr.toString().getBytes(StandardCharsets.UTF_8)));
                engine.getDb(client).set(client, key, hash);
                return new IntegerRedisMessage(incr);
            }
            Long newV = hash.incrBy(field, incr);
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            return new IntegerRedisMessage(newV);
        }
    }

    public static class HIncrByFloat extends ArgsCommand.FourExWith<RHash> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Key field = new Key(msg.getAt(2).bytes());
            Double incr = NumberUtils.parseDouble(msg.getAt(3).str());
            RHash hash = get(key);
            if (hash == null) {
                hash = new RHash(engine.timeProvider());
                hash.set(field, new Key(NumberUtils.formatDouble(incr).getBytes(StandardCharsets.UTF_8)));
                engine.getDb(client).set(client, key, hash);
                return FullBulkValueRedisMessage.ofDouble(incr);
            }
            Double newV = hash.incrByFloat(field, incr);
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            return FullBulkValueRedisMessage.ofDouble(newV);
        }
    }

    static RedisMessage transPair(List<RHash.Pair> r, boolean withValue) {
        if (r == null || r.isEmpty()) {
            return ListRedisMessage.empty();
        }
        List<RedisMessage> result = new LinkedList<>();
        for (RHash.Pair it : r) {
            result.add(FullBulkValueRedisMessage.ofString(it.field.getContent()));
            if (withValue) result.add(FullBulkValueRedisMessage.ofString(it.value.getContent()));
        }
        return new ListRedisMessage(result);
    }

    static RedisMessage hget(RHash hash, Key field) {
        if (hash == null) {
            return FullBulkValueRedisMessage.NULL_INSTANCE;
        }
        Key v = hash.get(field);
        if (v == null) {
            return FullBulkValueRedisMessage.NULL_INSTANCE;
        }
        return FullBulkValueRedisMessage.ofString(v.getContent());
    }

    private static List<RHash.Pair> parsePair(ListRedisMessage msg, int start, int end) {
        List<RHash.Pair> list = new LinkedList<>();
        for (int i = start; i < end - 1; i += 2) {
            if (i + 1 == msg.children().size()) {
                throw new RedisServerException(Constants.ERR_SYNTAX);
            }
            byte[] f = msg.getAt(i).bytes();
            byte[] value = msg.getAt(i + 1).bytes();
            list.add(new RHash.Pair(f, value));
        }
        return list;
    }
}
