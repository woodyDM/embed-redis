package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/4/29
 */
public interface RedisCommand {

    RedisMessage response(RedisEngine engine, RedisMessage message, ChannelHandlerContext ctx);
    
}
