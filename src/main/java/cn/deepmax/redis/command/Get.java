package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisBulkString;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class Get implements RedisCommand {

    @Override
    public RedisType response(RedisEngine engine, RedisType type, ChannelHandlerContext ctx) {

        if (type.size() < 2) {
            return new RedisError("invalid set size");
        }
        byte[] key = type.get(1).bytes();
        byte[] bytes = engine.get(key).orElse(null);
        return RedisBulkString.of(bytes);
    }


}
