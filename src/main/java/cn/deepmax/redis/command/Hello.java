package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisArray;
import cn.deepmax.redis.type.RedisBulkString;
import cn.deepmax.redis.type.RedisInteger;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

public class Hello implements RedisCommand{
    @Override
    public RedisType response(RedisEngine engine, RedisType type, ChannelHandlerContext ctx) {
        RedisArray array = new RedisArray();
        array.add(RedisBulkString.of("server"));
        array.add(RedisBulkString.of("redis"));
        array.add(RedisBulkString.of("proto"));
        array.add(new RedisInteger(2));

        return array;
    }
}
