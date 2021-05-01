package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisString;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

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
        byte[] key = m.get(1).bytes();
        byte[] value = m.get(2).bytes();
        engine.set(key,value);
        return new RedisString("OK");
    }


}
