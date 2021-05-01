package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.message.MessageWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/4/30
 */
public abstract class AbstractArrayCommand implements RedisCommand{

    @Override
    public RedisMessage response(RedisEngine engine, RedisMessage message, ChannelHandlerContext ctx) {
        ArrayRedisMessage m = (ArrayRedisMessage) message;
        ByteBuf buf = Unpooled.buffer();
        return response0(engine,new MessageWrapper(m),buf);
    }

    protected abstract RedisMessage response0(RedisEngine engine, MessageWrapper m, ByteBuf buf);
    
}
