package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ErrorRedisMessage;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class UnsupportedErrorCommand implements RedisCommand{
    @Override
    public RedisType response(RedisEngine engine, RedisType type, ChannelHandlerContext ctx) {
       return new RedisError("unsupported command");
    }
}
