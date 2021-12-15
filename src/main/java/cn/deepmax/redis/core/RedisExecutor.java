package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import io.netty.handler.codec.redis.RedisMessage;

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
    RedisMessage execute(RedisMessage type, RedisEngine engine, Redis.Client client);

}
