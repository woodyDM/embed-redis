package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisEngine;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
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
    RedisMessage execute(RedisMessage type, RedisEngine engine, Client client);

}
