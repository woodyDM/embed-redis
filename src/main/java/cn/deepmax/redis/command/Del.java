package cn.deepmax.redis.command;

import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisInteger;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class Del implements RedisCommand{
    @Override
    public RedisType response(RedisEngine engine, RedisType type, ChannelHandlerContext ctx) {

        if (type.children().size() < 2) {
            return new RedisError("ERR wrong number of arguments for 'del' command");
        }
        int c = 0;
        for (int i = 1; i < type.children().size(); i++) {
            boolean deleted = engine.del(type.get(i).bytes());
            if (deleted) {
                c++;
            }
        }
        return new RedisInteger(c);
    }

}
