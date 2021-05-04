package cn.deepmax.redis.engine.module;

import cn.deepmax.redis.engine.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.engine.support.BaseModule;
import cn.deepmax.redis.type.*;
import io.netty.channel.ChannelHandlerContext;

public class HandShakeModule extends BaseModule {
    public HandShakeModule() {
        super("handshake");
        register(new Hello());
        register(new Ping());
    }

    private static class Hello implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            RedisArray array = new RedisArray();
            array.add(RedisBulkString.of("server"));
            array.add(RedisBulkString.of("redis"));
            array.add(RedisBulkString.of("proto"));
            array.add(new RedisInteger(2));

            return array;
        }

    }

    private static class Ping implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            return new RedisString("PONG");
        }
    }

}
