package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.type.RedisType;

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
    public RedisType exec(RedisType type, Client client) {
        return engine.execute(type, client);
    }
}
