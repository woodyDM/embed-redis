package cn.deepmax.redis.base;

import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisEngineHolder;
import cn.deepmax.redis.core.DefaultRedisEngine;
import org.junit.Before;

public class BaseMemEngineTest implements EngineTest, TimedTest {

    public static final int PORT = 6382;
    public static final String AUTH = "123456";

    @Override
    public RedisEngine engine() {
        return RedisEngineHolder.instance();
    }

    @Override
    public String auth() {
        return AUTH;
    }

    @Before
    public void setUp() {
        DefaultRedisEngine e = DefaultRedisEngine.defaultEngine();
        e.setTimeProvider(TIME_PROVIDER);
        RedisConfiguration.Standalone standalone = new RedisConfiguration.Standalone(PORT, AUTH);
        e.setConfiguration(new RedisConfiguration("localhost",standalone, null));
        TIME_PROVIDER.reset();
        RedisEngineHolder.set(e);
    }

}
