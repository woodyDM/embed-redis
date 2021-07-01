package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.type.RedisString;
import cn.deepmax.redis.type.RedisType;

/**
 * @author wudi
 * @date 2021/4/29
 */
public interface RedisCommand  {
    /**
     * command for search
     *
     * @return
     */
    default String name() {
        return this.getClass().getSimpleName().toLowerCase();
    }
    
    RedisType response(RedisType type, Redis.Client client, RedisEngine engine);

    RedisType OK = new RedisString("OK");
    
}
