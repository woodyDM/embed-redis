package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.type.RedisType;

/**
 * @author wudi
 * @date 2021/6/28
 */
@FunctionalInterface
public interface RedisExecutor {
    /**
     * execute
     *
     * @param type
     * @param engine
     * @param client
     * @return
     */
    RedisType execute(RedisType type, RedisEngine engine, Redis.Client client);

}
