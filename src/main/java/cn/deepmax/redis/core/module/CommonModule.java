package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

public class CommonModule extends BaseModule {

    public CommonModule() {
        super("common");
        register(new Del());

    }

    private static class Del implements RedisCommand {
        //todo
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            ListRedisMessage msg = cast(type);
            if (msg.children().size() < 2) {
                return new ErrorRedisMessage("ERR wrong number of arguments for 'del' command");
            }
            int c = 0;
            for (int i = 1; i < msg.children().size(); i++) {
                RedisObject old = engine.getDbManager().get(client).del(msg.getAt(i).bytes());
                if (old != null && !engine.isExpire(old)) {
                    c++;
                }
            }
            return new IntegerRedisMessage(c);
        }
    }


}
