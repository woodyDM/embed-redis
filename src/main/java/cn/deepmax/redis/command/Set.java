package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.message.MessageWrapper;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisString;
import cn.deepmax.redis.type.RedisType;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class Set extends AbstractArrayCommand {
    @Override
    protected RedisType response0(RedisEngine engine, MessageWrapper m, ByteBuf buf) {
        if (m.size() < 3) {
            return new RedisError("invalid set size");
        }
        String key = m.getAt(1);
        String value = m.getAt(2);
        engine.set(key,value);
        return new RedisString("OK");
    }
}
