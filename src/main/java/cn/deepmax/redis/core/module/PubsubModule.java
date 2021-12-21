package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.PubsubManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class PubsubModule extends BaseModule {

    public PubsubModule() {
        super("pubsub");
        register(new Publish());
        register(new Subscribe());
        register(new PSubscribe());
        register(new Unsubscribe());
        register(new Punsubscribe());
    }

    private static class Publish implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            ListRedisMessage msg = cast(type);
            byte[] bytes = msg.getAt(1).bytes();
            Key channel = new Key(bytes);
            byte[] message = msg.getAt(2).bytes();
            int num = engine.pubsub().pub(channel, message);
            return new IntegerRedisMessage(num);
        }
    }

    private static class Subscribe extends BaseSubscribe {
        @Override
        PubsubManager.Pubsub select(PubsubManager manager) {
            return manager.direct();
        }
    }

    private static class PSubscribe extends BaseSubscribe {
        @Override
        PubsubManager.Pubsub select(PubsubManager manager) {
            return manager.pattern();
        }
    }

    abstract static class BaseSubscribe implements RedisCommand {

        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            ListRedisMessage msg = cast(type);
            List<RedisMessage> children = msg.children();
            if (children.size() <= 1) {
                return new ErrorRedisMessage("invalid sub size");
            }
            List<Key> channels = genKeys(children);
            List<RedisMessage> list = select(engine.pubsub()).sub(client, channels);
            return CompositeRedisMessage.of(list);
        }

        abstract PubsubManager.Pubsub select(PubsubManager manager);

    }

    private static List<Key> genKeys(List<RedisMessage> children) {
        List<Key> channels = new ArrayList<>(children.size() - 1);
        for (int i = 1; i < children.size(); i++) {
            channels.add(new Key(((FullBulkValueRedisMessage) children.get(i)).bytes()));
        }
        return channels;
    }

    private static class Unsubscribe extends BaseUnsubscribe {

        @Override
        protected PubsubManager.Pubsub select(PubsubManager pubsub) {
            return pubsub.direct();
        }
    }

    private static class Punsubscribe extends BaseUnsubscribe {

        @Override
        protected PubsubManager.Pubsub select(PubsubManager pubsub) {
            return pubsub.pattern();
        }
    }

    private static abstract class BaseUnsubscribe implements RedisCommand {

        abstract protected PubsubManager.Pubsub select(PubsubManager pubsub);

        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            //todo
            ListRedisMessage msg = cast(type);
            List<RedisMessage> children = msg.children();
            List<RedisMessage> result;
            if (children.size() == 1) {
                result = select(engine.pubsub()).unsubAll(client);
            } else {
                List<Key> keys = genKeys(msg.children());
                result = select(engine.pubsub()).unsub(client, keys);
            }
            return CompositeRedisMessage.of(result);
        }
    }
    
}
