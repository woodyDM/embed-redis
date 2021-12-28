package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.PubsubManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

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

    private static class Publish extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
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

    abstract static class BaseSubscribe extends ArgsCommand.Two {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<Key> channels = genKeys(msg.children(), 1);
            List<RedisMessage> list = select(engine.pubsub()).sub(client, channels);
            return CompositeRedisMessage.of(list);
        }

        abstract PubsubManager.Pubsub select(PubsubManager manager);

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

    private static abstract class BaseUnsubscribe extends ArgsCommand.One {

        abstract protected PubsubManager.Pubsub select(PubsubManager pubsub);

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<RedisMessage> children = msg.children();
            List<RedisMessage> result;
            if (children.size() == 1) {
                result = select(engine.pubsub()).unsubAll(client);
            } else {
                List<Key> keys = genKeys(msg.children(), 1);
                result = select(engine.pubsub()).unsub(client, keys);
            }
            return CompositeRedisMessage.of(result);
        }
    }
    
}
