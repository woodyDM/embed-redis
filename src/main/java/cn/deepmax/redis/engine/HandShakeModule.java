package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.*;
import io.netty.channel.ChannelHandlerContext;

public class HandShakeModule extends BaseModule{
    public HandShakeModule( ) {
        super("handshake");
        register(new Hello());
        register(new Ping());
    }

    private static class Hello implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx) {
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
        public RedisType response(RedisType type, ChannelHandlerContext ctx) {
            return new RedisString("PONG");
        }
    }

}
