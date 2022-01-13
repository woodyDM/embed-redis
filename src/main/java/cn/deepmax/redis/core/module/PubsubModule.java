package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.PubsubManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RPattern;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.CompositeCommand;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.Map;

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
        register(new CompositeCommand("pubsub")
                .with(new PubsubChannels())
                .with(new PubsubNumpat())
                .with(new PubsubNumsub())
        );
    }

    public static class PubsubChannels extends ArgsCommand<ArgsCommand.RVoid> {
        public PubsubChannels() {
            super(2, 3);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RPattern pattern = null;
            if (msg.children().size() == 3) pattern = RPattern.compile(msg.getAt(2).str());
            List<Key> channels = engine.pubsub().channelNumbers(pattern);
            return ListRedisMessage.wrapKeys(channels);
        }
    }

    public static class PubsubNumpat extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            long num = engine.pubsub().numberPattern();
            return new IntegerRedisMessage(num);
        }
    }

    public static class PubsubNumsub extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<Key> keys = genKeys(msg.children(), 2);
            Map<Key, Integer> numbersub = engine.pubsub().numbersub(keys);
            ListRedisMessage.Builder b = ListRedisMessage.newBuilder();
            for (Key key : keys) {
                b.append(FullBulkValueRedisMessage.ofString(key.getContent()));
                Integer num = numbersub.getOrDefault(key, 0);
                b.append(new IntegerRedisMessage(num));
            }
            return b.build();
        }
    }

    private static class Publish extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            Key channel = msg.getAt(1).key();
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
        protected PubsubManager.Pubsub pubsub() {
            return engine.pubsub().direct();
        }
    }

    private static class Punsubscribe extends BaseUnsubscribe {

        @Override
        protected PubsubManager.Pubsub pubsub() {
            return engine.pubsub().pattern();
        }
    }

    private static abstract class BaseUnsubscribe extends ArgsCommand.One {

        abstract protected PubsubManager.Pubsub pubsub();

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<RedisMessage> children = msg.children();
            List<RedisMessage> result;
            if (children.size() == 1) {
                result = pubsub().unsubAll(client);
            } else {
                List<Key> keys = genKeys(msg.children(), 1);
                result = pubsub().unsub(client, keys);
            }
            return CompositeRedisMessage.of(result);
        }
    }
    
}
