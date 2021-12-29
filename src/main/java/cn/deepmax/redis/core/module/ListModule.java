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
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

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
        register(new LMove());
        register(new LMPop());
        //blocking op
        register(new BLPop());
        register(new BRPop());
        register(new BLMPop());
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
                    () -> FullBulkStringRedisMessage.NULL_INSTANCE).register();
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

    public static class RPopLPush extends BasePopPush {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            return doResponse(msg, client, engine, RList::rPop, RList::lpush);
        }

        @Override
        public Optional<ErrorRedisMessage> preCheckLength(RedisMessage type) {
            return exactCheckLength(type, 3);
        }
    }

    public static class LMove extends BasePopPush {
        static String L = "left";
        static String R = "right";

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String sourceA = parse(msg, 3);
            String descA = parse(msg, 4);
            if (L.equals(sourceA) && L.equals(descA)) {
                return doResponse(msg, client, engine, RList::lPop, RList::lpush);
            } else if (R.equals(sourceA) && R.equals(descA)) {
                return doResponse(msg, client, engine, RList::rPop, RList::rpush);
            } else if (L.equals(sourceA) && R.equals(descA)) {
                return doResponse(msg, client, engine, RList::lPop, RList::rpush);
            } else if (R.equals(sourceA) && L.equals(descA)) {
                return doResponse(msg, client, engine, RList::rPop, RList::lpush);
            } else {
                return Constants.ERR_SYNTAX;
            }
        }

        private String parse(ListRedisMessage msg, int index) {
            String sourceA = msg.getAt(index).str().toLowerCase();
            if (L.equals(sourceA) || R.equals(sourceA)) {
                return sourceA;
            } else {
                throw new RedisServerException(Constants.ERR_SYNTAX);
            }
        }

        @Override
        public Optional<ErrorRedisMessage> preCheckLength(RedisMessage type) {
            return exactCheckLength(type, 5);
        }
    }

    abstract static class BasePopPush extends ArgsCommand.ThreeWith<RList> {
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine, Function<RList, Key> sourceAction,
                                          BiConsumer<RList, Key> destAction) {
            byte[] sourceKey = msg.getAt(1).bytes();
            RList source = get(sourceKey);
            byte[] destKey = msg.getAt(2).bytes();
            RList dest = get(destKey);
            if (source == null) {
                return FullBulkValueRedisMessage.NULL_INSTANCE;
            }
            Key value = sourceAction.apply(source);
            if (source.size() == 0) {
                engine.getDb(client).del(client, sourceKey);
            } else {
                engine.fireChangeEvent(client, sourceKey, DbManager.EventType.UPDATE);
            }
            if (dest == null) {
                dest = new RList(engine.timeProvider());
                engine.getDb(client).set(client, destKey, dest);
                destAction.accept(dest, value);
            } else {
                destAction.accept(dest, value);
                engine.fireChangeEvent(client, destKey, DbManager.EventType.UPDATE);
            }
            return FullBulkValueRedisMessage.ofString(value.getContent());
        }
    }


}

