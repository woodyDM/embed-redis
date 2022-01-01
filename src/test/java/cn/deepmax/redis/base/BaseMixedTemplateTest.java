package cn.deepmax.redis.base;

import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisEngineHolder;
import cn.deepmax.redis.core.DefaultRedisEngine;
import cn.deepmax.redis.support.EmbedRedisRunner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * test use engine and template
 */
@RunWith(Parameterized.class)
public abstract class BaseMixedTemplateTest extends BaseTemplateTest implements EngineTest, TimedTest {
    public static final String AUTH = "123456";
    public static final String HOST = "localhost";
    public static final int PORT = 6381;
    public static Client[] ts;

    protected static DefaultRedisEngine engine;

    public BaseMixedTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Parameterized.Parameters
    public static Collection<RedisTemplate<String, Object>> prepareTemplate() {
        return Arrays.stream(ts).map(c -> c.t).collect(Collectors.toList());
    }

    static {
        try {
            init();
        } catch (Exception e) {
            log.error("启动失败", e);
            throw new IllegalStateException(e);
        }
    }

    private static void init() {
        engine = EmbedRedisRunner.start(PORT, AUTH);
        ts = new Client[3];
        init(ts, HOST, PORT, AUTH);
    }

    @Before
    public void setUp() {
        RedisEngineHolder.set(engine());
        engine().flush();
    }

    @Override
    public String auth() {
        return AUTH;
    }

    @Override
    public RedisEngine engine() {
        return engine;
    }

}
