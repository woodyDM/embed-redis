package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

/**
 * @author wudi
 * @date 2021/4/29
 */
public class PingCommand implements RedisCommand {
    @Override
    public RedisMessage response(RedisEngine engine, RedisMessage message, ChannelHandlerContext ctx) {
        return new SimpleStringRedisMessage("PONG");
    }
}
