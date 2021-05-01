package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisString;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

/**
 * @author wudi
 * @date 2021/4/29
 */
public class Ping implements RedisCommand {
    @Override
    public RedisType response(RedisEngine engine, RedisType type, ChannelHandlerContext ctx) {
        return new RedisString("PONG");
    }
}
