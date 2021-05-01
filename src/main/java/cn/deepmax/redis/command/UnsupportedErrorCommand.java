package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class UnsupportedErrorCommand implements RedisCommand{
    @Override
    public RedisMessage response(RedisEngine engine, RedisMessage message, ChannelHandlerContext ctx) {
       return new ErrorRedisMessage("unsupported command");
    }
}
