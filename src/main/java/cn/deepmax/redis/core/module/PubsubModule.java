package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.PubsubManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
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
            Key[] channels = new Key[children.size() - 1];
            for (int i = 1; i < children.size(); i++) {
                channels[i - 1] = new Key(msg.getAt(i).bytes());
            }
            List<RedisMessage> list = select(engine.pubsub()).sub(client, channels);
            return CompositeRedisMessage.of(list);
        }

        abstract PubsubManager.Pubsub select(PubsubManager manager);

    }

    private static class Unsubscribe implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            //todo
//            ListRedisMessage msg = cast(type);
//            List<RedisMessage> children = msg.children();
//            if (children.size() <= 1) {
//                return new ErrorRedisMessage("invalid unsub size");
//            }
//            Key[] channels = new Key[children.size() - 1];
//            for (int i = 1; i < children.size(); i++) {
//                channels[i - 1] = new Key(msg.getAt(i).bytes());
//            }
            return null;

        }
    }



}
