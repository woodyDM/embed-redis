package cn.deepmax.redis.engine.module;

import cn.deepmax.redis.engine.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.engine.RedisObject;
import cn.deepmax.redis.engine.support.BaseModule;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisInteger;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

public class CommonModule extends BaseModule {

    public CommonModule( ) {
        super("common");
        register(new Del());

    }

    private static class Del implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            if (type.children().size() < 2) {
                return new RedisError("ERR wrong number of arguments for 'del' command");
            }
            int c = 0;
            for (int i = 1; i < type.children().size(); i++) {
                RedisObject old = engine.del(type.get(i).bytes());
                if (old != null && !engine.isExpire(old)) {
                    c++;
                }
            }
            return new RedisInteger(c);
        }
    }



}
