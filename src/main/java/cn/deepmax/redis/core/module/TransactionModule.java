package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

/**
 * @author wudi
 * @date 2021/12/24
 */
public class TransactionModule extends BaseModule {
    public TransactionModule() {
        super("transaction");
        register(new Multi());
        register(new Watch());
        register(new Exec());
        register(new Unwatch());
        register(new Discard());
    }

    public static class Watch extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            if (client.queued()) {
                return new ErrorRedisMessage("ERR WATCH inside MULTI is not allowed");
            }
            List<Key> keys = genKeys(msg.children(), 1);
            engine.transactionManager().watch(client, keys);
            return OK;
        }
    }

    public static class Unwatch extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            if (client.queued()) {
                return Constants.QUEUED;
            }
            engine.transactionManager().unwatch(client);
            return OK;
        }
    }

    public static class Multi extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            if (client.queued()) {
                return new ErrorRedisMessage("ERR MULTI calls can not be nested");
            }
            engine.transactionManager().multi(client);
            return OK;
        }
    }

    public static class Exec extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            if (!client.queued()) {
                return new ErrorRedisMessage("ERR EXEC without MULTI");
            }
            return engine.transactionManager().exec(client);
        }
    }

    public static class Discard extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            if (!client.queued()) {
                return new ErrorRedisMessage("ERR DISCARD without MULTI");
            }
            client.setQueue(false);
            engine.transactionManager().unwatch(client);
            return OK;
        }
    }
}
