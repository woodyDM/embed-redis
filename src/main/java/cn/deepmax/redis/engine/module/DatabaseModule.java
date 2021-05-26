package cn.deepmax.redis.engine.module;

import cn.deepmax.redis.engine.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.engine.support.BaseModule;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wudi
 * @date 2021/5/26
 */
public class DatabaseModule extends BaseModule {
    public DatabaseModule() {
        super("database");
        register(new Select());

    }

    private static class Select implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            int i = Integer.parseInt(type.get(1).str());
            engine.getDbManager().switchTo(ctx.channel(), i);
            return OK;
        }
    }

}
