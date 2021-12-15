package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import io.netty.handler.codec.redis.RedisMessage;

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
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            String idx = cast(type).getAt(1).str();
            int i = Integer.parseInt(idx);
            engine.getDbManager().switchTo(client, i);
            return OK;
        }
    }

}
