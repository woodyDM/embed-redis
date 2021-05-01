package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.message.MessageWrapper;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisString;
import cn.deepmax.redis.type.RedisType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class Set implements RedisCommand {
    @Override
    public RedisType response(RedisEngine engine, RedisType m, ChannelHandlerContext ctx) {
        if (m.size() < 3) {
            return new RedisError("invalid set size");
        }
        String key = m.get(1).str();
        String value = m.get(2).str();
        engine.set(key,value);
        return new RedisString("OK");
    }


}
