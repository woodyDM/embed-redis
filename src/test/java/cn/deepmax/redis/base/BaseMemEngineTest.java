package cn.deepmax.redis.base;

import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.DefaultRedisEngine;
import org.junit.Before;

public class BaseMemEngineTest implements EngineTest, TimedTest {

    public static final int PORT = 6382;
    public static final String AUTH = "123456";
    protected DefaultRedisEngine engine;

    @Override
    public RedisEngine engine() {
        return engine;
    }

    @Override
    public String auth() {
        return AUTH;
    }

    @Before
    public void setUp() {
        engine = DefaultRedisEngine.defaultEngine();
        engine.setTimeProvider(TIME_PROVIDER);
        RedisConfiguration.Standalone standalone = new RedisConfiguration.Standalone(PORT, AUTH);
        engine.setConfiguration(new RedisConfiguration("localhost", standalone, null));
        TIME_PROVIDER.reset();
    }

}
