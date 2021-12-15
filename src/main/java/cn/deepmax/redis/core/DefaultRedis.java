package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/6/25
 */
public class DefaultRedis implements Redis {

    RedisEngine engine;

    public DefaultRedis(RedisEngine engine) {
        this.engine = engine;
    }

    @Override
    public RedisConfiguration configuration() {
        return engine.configuration();
    }

    @Override
    public void setConfiguration(RedisConfiguration configuration) {
        Objects.requireNonNull(configuration);
        engine.setConfiguration(configuration);
    }

    @Override
    public RedisMessage exec(RedisMessage type, Client client) {
        return engine.execute(type, client);
    }
}
