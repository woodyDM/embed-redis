package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.Optional;

/**
 * @author wudi
 * @date 2021/12/27
 */
public class ListModule extends BaseModule {
    public ListModule() {
        super("list");
        register(new LPush());
        register(new BLPop());
    }

    public static class LPush extends ArgsCommand.ThreeWith<RList> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> keys = genKeys(msg.children(), 2);
            RList list = get(key);
            if (list == null) {
                list = new RList(engine.timeProvider());
                engine.getDb(client).set(client, key, list);
            }
            keys.forEach(list::addFirst);
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            return new IntegerRedisMessage(list.size());
        }
    }

    public static class BLPop extends ArgsCommand.ThreeWith<RList> {
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
                Key value = obj.lPop();
                if (value == null) {
                    engine.getDb(client).del(client, k.getContent());
                    return FullBulkStringRedisMessage.NULL_INSTANCE;
                } else {
                    engine.fireChangeEvent(client, k.getContent(), DbManager.EventType.UPDATE);
                    return FullBulkValueRedisMessage.ofString(value.getContent());
                }
            });
        }
    }

}

