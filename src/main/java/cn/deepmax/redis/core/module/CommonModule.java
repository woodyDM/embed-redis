package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisInteger;
import cn.deepmax.redis.type.RedisType;

public class CommonModule extends BaseModule {

    public CommonModule( ) {
        super("common");
        register(new Del());

    }

    private static class Del implements RedisCommand {
        @Override
        public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
            if (type.children().size() < 2) {
                return new RedisError("ERR wrong number of arguments for 'del' command");
            }
            int c = 0;
            for (int i = 1; i < type.children().size(); i++) {
                RedisObject old = engine.getDbManager().get(client ).del(type.get(i).bytes());
                if (old != null && !engine.isExpire(old)) {
                    c++;
                }
            }
            return new RedisInteger(c);
        }
    }



}
