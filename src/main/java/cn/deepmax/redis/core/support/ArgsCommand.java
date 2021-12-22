package cn.deepmax.redis.core.support;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/12/21
 */
public abstract class ArgsCommand implements RedisCommand {

    protected int limit;

    public ArgsCommand(int limit) {
        this.limit = limit;
    }

    @Override
    public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
        ListRedisMessage msg = cast(type);
        if (msg.children().size() < limit) {
            return new ErrorRedisMessage("ERR wrong number of arguments for '"
                    + this.getClass().getSimpleName().toLowerCase() + "' command");
        }
        return doResponse(msg, client, engine);
    }

    abstract protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine);


    public abstract static class Two extends ArgsCommand {
        public Two() {
            super(2);
        }
    }

    public abstract static class Three extends ArgsCommand {
        public Three() {
            super(3);
        }
    }
} 
    
