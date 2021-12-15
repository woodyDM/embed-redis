package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.PubsubManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

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
    }

    private static class Publish implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
//            byte[] bytes = type.get(1).bytes();
//            Key channel = new Key(bytes);
//            byte[] message = type.get(2).bytes();
//            int num = engine.pubsub().pub(channel, message);
//            return new RedisInteger(num);
            return new IntegerRedisMessage(1);
        }

    }

    private static class Subscribe extends BaseSubscribe {
        @Override
        PubsubManager.Pubsub select(PubsubManager manager) {
            return manager.normal();
        }
    }

    private static class PSubscribe extends BaseSubscribe {
        @Override
        PubsubManager.Pubsub select(PubsubManager manager) {
            return manager.regex();
        }
    }

    abstract static class BaseSubscribe implements RedisCommand {
        protected String name;

        BaseSubscribe() {
            this.name = this.getClass().getSimpleName().toLowerCase();
        }

        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
//            if (type.size() <= 1) {
//                return new RedisError("invalid sub size");
//            }
//            Key[] channels = new Key[type.size() - 1];
//            for (int i = 1; i < type.size(); i++) {
//                channels[i - 1] = new Key(type.get(i).bytes());
//            }
//            select(engine.pubsub()).sub(client, channels);
//            CompositeRedisType composite = new CompositeRedisType();
//            for (int i = 0; i < channels.length; i++) {
//                Key k = channels[i];
//                RedisArray ar = new RedisArray();
//                composite.add(ar);
//
//                ar.add(RedisBulkString.of(name));
//                ar.add(RedisBulkString.of(k.getContent()));
//                ar.add(new RedisInteger(i + 1));
//
//            }
//            return composite;
            return OK;
        }

        abstract PubsubManager.Pubsub select(PubsubManager manager);

    }


}
