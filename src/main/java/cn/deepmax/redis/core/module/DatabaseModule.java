package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.type.RedisType;

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
        public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
            int i = Integer.parseInt(type.get(1).str());
            engine.getDbManager().switchTo(client, i);
            return OK;
        }
    }

}
