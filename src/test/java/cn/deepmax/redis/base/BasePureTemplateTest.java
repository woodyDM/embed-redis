package cn.deepmax.redis.base;

import cn.deepmax.redis.support.EmbedRedisRunner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * test only use template
 */
@RunWith(Parameterized.class)
public abstract class BasePureTemplateTest extends BaseTemplateTest {
    public static final String AUTH = "123456";
    public static final String HOST = "localhost";
    public static final int PORT = 6381;
    public static Client[] ts;

    public BasePureTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Parameterized.Parameters
    public static Collection<RedisTemplate<String, Object>> prepareTemplate() {
        return Arrays.stream(ts).map(c -> c.t).collect(Collectors.toList());
    }

    public static boolean isEmbededRedis() {
        return PORT != 6379;
    }

    static {
        try {
            if (isEmbededRedis()) {
                EmbedRedisRunner.start(PORT, AUTH);
            }
            init();
        } catch (Exception e) {
            log.error("启动失败", e);
            throw new IllegalStateException(e);
        }
    }

    private static void init() {
        ts = new Client[3];
        init(ts, HOST, PORT, AUTH);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Before
    public void setUp() {
        t().execute((RedisCallback) con -> {
            con.flushAll();
            con.scriptingCommands().scriptFlush();
            return null;
        });
    }
    
}
