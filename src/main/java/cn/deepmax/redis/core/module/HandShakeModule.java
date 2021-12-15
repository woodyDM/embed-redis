package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

public class HandShakeModule extends BaseModule {
    public HandShakeModule() {
        super("handshake");
        register(new Hello());
        register(new Ping());
    }

    private static class Hello implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
//            if (type.size() == 1) {
//                RedisArray array = new RedisArray();
//                array.add(RedisBulkString.of("server"));
//                array.add(RedisBulkString.of("redis"));
//                array.add(RedisBulkString.of("proto"));
//                array.add(new RedisInteger(2));
//                return array;
//            } else {
//                RedisType v = type.get(1);
//                return new RedisError("NOPROTO unsupported protocol version");
//            }
            return OK;
        }

    }

    private static class Ping implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            return new SimpleStringRedisMessage("PONG");
        }
    }

}
