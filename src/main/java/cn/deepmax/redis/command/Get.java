package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.message.MessageWrapper;
import cn.deepmax.redis.message.Messages;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class Get extends AbstractArrayCommand {

    @Override
    protected RedisMessage response0(RedisEngine engine, MessageWrapper m, ByteBuf buf) {
        if (m.size() < 2) {
            return new ErrorRedisMessage("invalid set size");
        }
        String key = m.getAt(1);
        String v = engine.get(key).orElse(null);
        return Messages.bulkString(v);
    }
}
