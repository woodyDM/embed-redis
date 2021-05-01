package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.message.MessageWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class Del extends AbstractArrayCommand {

    @Override
    protected RedisMessage response0(RedisEngine engine, MessageWrapper m, ByteBuf buf) {
        if (m.size() < 2) {
            return new ErrorRedisMessage("ERR wrong number of arguments for 'del' command");
        }
        int c = 0;
        for (int i = 1; i < m.size(); i++) {
            boolean deleted = engine.del(m.getAt(i));
            if (deleted) {
                c++;
            }
        }
        return new IntegerRedisMessage(c);
    }
}
