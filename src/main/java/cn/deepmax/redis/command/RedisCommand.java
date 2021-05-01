package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wudi
 * @date 2021/4/29
 */
public interface RedisCommand {

    RedisType response(RedisEngine engine, RedisType type, ChannelHandlerContext ctx);
    
}
