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
import cn.deepmax.redis.utils.Tuple;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/12/27
 */
public class ListModule extends BaseModule {

    public ListModule() {
        super("list");
        register(new LPush());
        register(new LPushX());
        register(new RPush());
        register(new RPushX());
        register(new LPop());
        register(new RPop());
        register(new RPopLPush());
        register(new LLen());
        LMove lmove = new LMove();
        register(lmove);
        register(new BLMove(lmove));
        register(new LMPop());
        register(new LPos());
        register(new LRange());
        register(new LInsert());
        register(new LIndex());
        register(new LSet());
        register(new LRem());
        register(new LTrim());
        //blocking op
        register(new BLPop());
        register(new BRPop());
        register(new BLMPop());
        register(new BRPopLPush());

    }

    public static class LPos extends ArgsCommand<RList> {
        public LPos() {
            super(3, 5, 7, 9);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] ele = msg.getAt(2).bytes();
            //param check
            Optional<Long> rank = ArgParser.parseLongArg(msg, "rank");
            if (rank.isPresent() && rank.get() == 0) {
                return new ErrorRedisMessage("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second, ...");
            }
            Optional<Long> count = ArgParser.parseLongArg(msg, "count");
            Optional<Long> maxlen = ArgParser.parseLongArg(msg, "maxlen");
            RList list = get(key);
            if (list == null) {
                return FullBulkValueRedisMessage.NULL_INSTANCE;
            }
            List<Integer> pos = list.lpos(ele, rank, count, maxlen);
            if (!count.isPresent()) {
                if (pos.isEmpty()) {
                    return FullBulkValueRedisMessage.NULL_INSTANCE;
                } else {
                    return new IntegerRedisMessage(pos.get(0));
                }
            } else {
                List<RedisMessage> msgList = pos.stream()
                        .map(IntegerRedisMessage::new)
                        .collect(Collectors.toList());
                return new ListRedisMessage(msgList);
            }
        }
    }

    public static class LRange extends ArgsCommand.FourExWith<RList> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long start = NumberUtils.parse(msg.getAt(2).str());
            long end = NumberUtils.parse(msg.getAt(3).str());
            RList list = get(key);
            if (list == null) {
                return Constants.LIST_EMPTY;
            }
            List<Key> values = list.lrange((int) start, (int) end);
            List<RedisMessage> r = values.stream().map(k -> FullBulkValueRedisMessage.ofString(k.getContent()))
                    .collect(Collectors.toList());
            return new ListRedisMessage(r);
        }
    }

    public static class LInsert extends ArgsCommand<RList> {
        static final String BEFORE = "before";
        static final String AFTER = "after";

        public LInsert() {
            super(5, true);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            String p = msg.getAt(2).str().toLowerCase();
            int offset;
            if (BEFORE.equals(p)) {
                offset = 0;
            } else if (AFTER.equals(p)) {
                offset = 1;
            } else {
                return Constants.ERR_SYNTAX;
            }
            RList list = get(key);
            if (list == null) {
                return Constants.INT_ZERO;
            }
            byte[] pivot = msg.getAt(3).bytes();
            byte[] ele = msg.getAt(4).bytes();
            int num = list.insert(pivot, ele, offset);
            if (num != -1) {
                engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            }
            return new IntegerRedisMessage(num);
        }
    }

    public static class LIndex extends ArgsCommand.ThreeExWith<RList> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long idx = NumberUtils.parse(msg.getAt(2).str());
            RList list = get(key);
            if (list == null) {
                return FullBulkValueRedisMessage.NULL_INSTANCE;
            }
            Key v = list.valueAt((int) idx);
            if (v == null) {
                return FullBulkValueRedisMessage.NULL_INSTANCE;
            } else {
                return FullBulkValueRedisMessage.ofString(v.getContent());
            }
        }
    }

    public static class LSet extends ArgsCommand.FourExWith<RList> {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long idx = NumberUtils.parse(msg.getAt(2).str());
            byte[] ele = msg.getAt(3).bytes();
            RList list = get(key);
            if (list == null) {
                return new ErrorRedisMessage("ERR no such key");
            }
            int effected = list.lset((int) idx, ele);
            if (effected == -1) {
                return new ErrorRedisMessage("ERR index out of range");
            } else {
                engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
                return OK;
            }
        }
    }

    public static class LRem extends ArgsCommand.FourExWith<RList> {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long count = NumberUtils.parse(msg.getAt(2).str());
            byte[] ele = msg.getAt(3).bytes();
            RList list = get(key);
            if (list == null) {
                return Constants.INT_ZERO;
            }
            int removed = list.remove(ele, (int) count);
            if (removed != 0) {
                if (list.size() == 0) {
                    engine.getDb(client).del(client, key);
                } else {
                    engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
                }
            }
            return new IntegerRedisMessage(removed);
        }
    }

    public static class LTrim extends ArgsCommand.FourExWith<RList> {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long start = NumberUtils.parse(msg.getAt(2).str());
            long stop = NumberUtils.parse(msg.getAt(3).str());
            RList list = get(key);
            if (list == null) {
                return OK;
            }
            list.trim((int) start, (int) stop);
            if (list.size() == 0) {
                engine.getDb(client).del(client, key);
            } else {
                engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            }
            return OK;
        }
    }

    public static class LMPop extends ArgsCommand.FourWith<RList> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            return Constants.ERR_NOT_SUPPORT;
        }
    }

    public static class BLMPop extends ArgsCommand.FourWith<RList> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            return Constants.ERR_NOT_SUPPORT;
        }
    }

    public static class LLen extends ArgsCommand.TwoExWith<RList> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RList list = get(msg.getAt(1).bytes());
            if (list == null) {
                return Constants.INT_ZERO;
            }
            return new IntegerRedisMessage(list.size());
        }
    }

    public static class LPush extends BasePush {
        public LPush() {
            super(false);
        }

        @Override
        protected void push(RList list, Key ele) {
            list.lpush(ele);
        }
    }

    public static class LPushX extends BasePush {
        public LPushX() {
            super(true);
        }

        @Override
        protected void push(RList list, Key ele) {
            list.lpush(ele);
        }
    }

    public static class RPush extends BasePush {

        public RPush() {
            super(false);
        }

        @Override
        protected void push(RList list, Key ele) {
            list.rpush(ele);
        }
    }

    public static class RPushX extends BasePush {
        public RPushX() {
            super(true);
        }

        @Override
        protected void push(RList list, Key ele) {
            list.rpush(ele);
        }
    }

    public abstract static class BasePush extends ArgsCommand.ThreeWith<RList> {

        protected final boolean existFlag;

        abstract protected void push(RList list, Key ele);

        public BasePush(boolean existFlag) {
            this.existFlag = existFlag;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> keys = genKeys(msg.children(), 2);
            RList list = get(key);
            if (list == null) {
                if (existFlag) {
                    //only push when exist
                    return Constants.INT_ZERO;
                } else {
                    list = new RList(engine.timeProvider());
                    engine.getDb(client).set(client, key, list);
                }
            }
            for (Key k : keys) {
                push(list, k);
            }
            //should get size before fire events
            long size = list.size();
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            return new IntegerRedisMessage(size);
        }
    }

    public static class LPop extends BasePop {
        @Override
        protected List<Key> pop(RList list, int count) {
            return list.lPop(count);
        }
    }

    public static class RPop extends BasePop {
        @Override
        protected List<Key> pop(RList list, int count) {
            return list.rPop(count);
        }
    }

    public abstract static class BasePop extends ArgsCommand<RList> {

        public BasePop() {
            super(2, 3);
        }

        abstract protected List<Key> pop(RList list, int count);


        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long count;
            boolean countFlag;
            if (msg.children().size() == 3) {
                count = NumberUtils.parse(msg.getAt(2).str());
                countFlag = true;
            } else {
                count = 1L;
                countFlag = false;
            }
            return tryPop(key, client, count.intValue(), countFlag);
        }

        private RedisMessage tryPop(byte[] key, Client client, int count, boolean countFlag) {
            RList list = get(key);
            if (list == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            List<Key> values = pop(list, count);
            if (list.size() == 0) {
                engine.getDb(client).del(client, key);
            } else {
                engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            }
            if (countFlag) {
                List<RedisMessage> msgList = values.stream()
                        .map(v -> FullBulkValueRedisMessage.ofString(v.getContent()))
                        .collect(Collectors.toList());
                return new ListRedisMessage(msgList);
            } else {
                return values.isEmpty() ? FullBulkValueRedisMessage.NULL_INSTANCE : FullBulkValueRedisMessage.ofString(values.get(0).getContent());
            }
        }

    }

    public static class BLPop extends BaseBPop {
        @Override
        protected Key pop(RList list) {
            return list.lPop();
        }
    }

    public static class BRPop extends BaseBPop {
        @Override
        protected Key pop(RList list) {
            return list.rPop();
        }
    }

    public abstract static class BaseBPop extends ArgsCommand.ThreeWith<RList> {

        abstract protected Key pop(RList list);

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<Key> keys = genKeys(msg.children(), 1, msg.children().size() - 1);
            Long timeout = NumberUtils.parseTimeout(msg.getAt(msg.children().size() - 1).str());
            Optional<RedisMessage> returnMsg = tryLPop(keys, client);
            if (returnMsg.isPresent()) {
                return returnMsg.get();
            }
            new BlockTask(client, keys, timeout, engine,
                    () -> tryLPop(keys, client),
                    () -> FullBulkStringRedisMessage.NULL_INSTANCE).block();
            return null;
        }

        private Optional<RedisMessage> tryLPop(List<Key> keys, Client client) {
            Optional<Key> exist = keys.stream().filter(k -> {
                RList obj = get(k.getContent());
                return obj != null && obj.size() > 0;
            }).findFirst();
            return exist.map(k -> {
                RList obj = get(exist.get().getContent());
                Key value = pop(obj);
                ListRedisMessage.Builder builder = ListRedisMessage.newBuilder();
                if (obj.size() == 0) {
                    engine.getDb(client).del(client, k.getContent());
                } else {
                    engine.fireChangeEvent(client, k.getContent(), DbManager.EventType.UPDATE);
                }
                if (value == null) {
                    return builder.append(FullBulkStringRedisMessage.NULL_INSTANCE).build();
                } else {
                    builder.append(FullBulkValueRedisMessage.ofString(k.getContent()));
                    builder.append(FullBulkValueRedisMessage.ofString(value.getContent()));
                    return builder.build();
                }
            });
        }
    }

    public static class BRPopLPush extends BasePopPush {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] sourceKey = msg.getAt(1).bytes();
            byte[] destKey = msg.getAt(2).bytes();
            Optional<RedisMessage> fetch = doResponseO(client, engine, RList::rPop, RList::lpush, sourceKey, destKey);
            if (fetch.isPresent()) {
                return fetch.get();
            }

            Long timeout = NumberUtils.parseTimeout(msg.getAt(3).str());
            new BlockTask(client, Collections.singletonList(new Key(sourceKey)), timeout, engine,
                    () -> doResponseO(client, engine, RList::rPop, RList::lpush, sourceKey, destKey),
                    () -> FullBulkValueRedisMessage.NULL_INSTANCE).block();
            return null;
        }

        @Override
        public Optional<ErrorRedisMessage> preCheckLength(RedisMessage type) {
            return exactCheckLength(type, 4);
        }
    }

    public static class RPopLPush extends BasePopPush {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] source = msg.getAt(1).bytes();
            byte[] dest = msg.getAt(2).bytes();
            return doResponseO(client, engine, RList::rPop, RList::lpush, source, dest)
                    .orElse(FullBulkValueRedisMessage.NULL_INSTANCE);
        }

        @Override
        public Optional<ErrorRedisMessage> preCheckLength(RedisMessage type) {
            return exactCheckLength(type, 3);
        }
    }

    public static class BLMove extends BasePopPush {
        final LMove lMove;

        BLMove(LMove lMove) {
            this.lMove = lMove;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            if (client.queued()) {
                return lMove.doResponse(msg, client, engine);
            }
            byte[] source = msg.getAt(1).bytes();
            byte[] destKey = msg.getAt(2).bytes();
            String sourceA = parse(msg, 3);
            String descA = parse(msg, 4);
            Tuple<Function<RList, Key>, BiConsumer<RList, Key>> t = toAction(sourceA, descA);
            //first try
            Optional<RedisMessage> fetched = doResponseO(client, engine, t.a, t.b, source, destKey);
            if (fetched.isPresent()) {
                return fetched.get();
            }
            //block to get 
            Long timeout = NumberUtils.parseTimeout(msg.getAt(5).str());
            new BlockTask(client, Collections.singletonList(new Key(source)), timeout, engine,
                    () -> doResponseO(client, engine, t.a, t.b, msg.getAt(1).bytes(), destKey),
                    () -> FullBulkValueRedisMessage.NULL_INSTANCE).block();
            return null;
        }

        @Override
        protected Optional<ErrorRedisMessage> exactCheckLength(RedisMessage type, int size) {
            return exactCheckLength(type, 6);
        }
    }

    public static class LMove extends BasePopPush {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String sourceA = parse(msg, 3);
            String descA = parse(msg, 4);
            Tuple<Function<RList, Key>, BiConsumer<RList, Key>> t = toAction(sourceA, descA);
            byte[] source = msg.getAt(1).bytes();
            byte[] dest = msg.getAt(2).bytes();
            return doResponseO(client, engine, t.a, t.b, source, dest)
                    .orElse(FullBulkValueRedisMessage.NULL_INSTANCE);
        }

        @Override
        public Optional<ErrorRedisMessage> preCheckLength(RedisMessage type) {
            return exactCheckLength(type, 5);
        }
    }

    abstract static class BasePopPush extends ArgsCommand.ThreeWith<RList> {
        static String L = "left";
        static String R = "right";

        protected Tuple<Function<RList, Key>, BiConsumer<RList, Key>> toAction(String sourceA, String descA) {
            if (L.equals(sourceA) && L.equals(descA)) {
                return new Tuple<>(RList::lPop, RList::lpush);
            } else if (R.equals(sourceA) && R.equals(descA)) {
                return new Tuple<>(RList::rPop, RList::rpush);
            } else if (L.equals(sourceA) && R.equals(descA)) {
                return new Tuple<>(RList::lPop, RList::rpush);
            } else if (R.equals(sourceA) && L.equals(descA)) {
                return new Tuple<>(RList::rPop, RList::lpush);
            } else {
                throw new IllegalStateException("invalid " + sourceA + " " + descA);
            }
        }

        protected String parse(ListRedisMessage msg, int index) {
            String sourceA = msg.getAt(index).str().toLowerCase();
            if (L.equals(sourceA) || R.equals(sourceA)) {
                return sourceA;
            } else {
                throw new RedisServerException(Constants.ERR_SYNTAX);
            }
        }

        protected Optional<RedisMessage> doResponseO(Client client, RedisEngine engine, Function<RList, Key> sourceAction,
                                                     BiConsumer<RList, Key> destAction, byte[] sourceBytes, byte[] destBytes) {
            RList source = get(sourceBytes);
            RList dest = get(destBytes);
            if (source == null) {
                return Optional.empty();
            }
            Key value = sourceAction.apply(source);
            if (source.size() == 0) {
                engine.getDb(client).del(client, sourceBytes);
            } else {
                engine.fireChangeEvent(client, sourceBytes, DbManager.EventType.UPDATE);
            }
            if (dest == null) {
                dest = new RList(engine.timeProvider());
                engine.getDb(client).set(client, destBytes, dest);
                destAction.accept(dest, value);
            } else {
                destAction.accept(dest, value);
                engine.fireChangeEvent(client, destBytes, DbManager.EventType.UPDATE);
            }
            return Optional.of(FullBulkValueRedisMessage.ofString(value.getContent()));
        }
    }

}

