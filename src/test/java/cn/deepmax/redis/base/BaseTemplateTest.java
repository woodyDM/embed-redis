package cn.deepmax.redis.base;

import cn.deepmax.redis.RedisServer;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.DefaultRedisEngine;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.redisson.spring.data.connection.RedissonConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public abstract class BaseTemplateTest extends BaseTest {

    public static final String AUTH = "123456";
    public static final String HOST = "localhost";
    public static final int PORT = 6381;
    public static Client[] ts;
    protected static RedisServer server;
    protected static DefaultRedisEngine engine;
    protected RedisTemplate<String, Object> redisTemplate;
    protected JdkSerializationRedisSerializer serializer = new JdkSerializationRedisSerializer();
    public static final Logger log = LoggerFactory.getLogger(BaseTemplateTest.class);
    protected static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final int POOL_SIZE = 4;

    static {
        scheduler.submit(() -> System.out.println("warn up"));
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    String auth() {
        return AUTH;
    }

    @Override
    public RedisEngine engine() {
        return engine;
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
        engine = DefaultRedisEngine.defaultEngine();
        engine.setTimeProvider(timeProvider);
        server = new RedisServer(engine, new RedisConfiguration(PORT, AUTH));
        if (PORT != 6379) {
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop();
            }));
        }
        ts = new Client[3];
        ts[0] = createRedisson();
        ts[1] = createLettuce();
        ts[2] = createJedis();
    }

    @Parameterized.Parameters
    public static Collection<RedisTemplate<String, Object>> prepareTemplate() {
        return Arrays.stream(ts).map(c -> c.t).collect(Collectors.toList());
    }

    public BaseTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    private static Client createJedis() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setDatabase(0);
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setPassword(AUTH);

        JedisClientConfiguration jedisClientConfiguration = JedisClientConfiguration.defaultConfiguration();
        jedisClientConfiguration.getPoolConfig().ifPresent(c -> {
            c.setMaxIdle(POOL_SIZE);
            c.setMaxTotal(POOL_SIZE);
            c.setMinIdle(POOL_SIZE);
        });
        JedisConnectionFactory factory = new JedisConnectionFactory(config, jedisClientConfiguration);
        return new Client(template(factory), factory);
    }

    private static Client createLettuce() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setDatabase(0);
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setPassword(AUTH);

        LettuceClientConfiguration lettuceClientConfiguration = LettuceClientConfiguration.builder()
                .clientResources(ClientResources.builder()
                        .build())
                .clientOptions(ClientOptions.builder()
                        .protocolVersion(ProtocolVersion.RESP2)
                        .build()).build();

        LettuceConnectionFactory factory = new LettuceConnectionFactory(config, lettuceClientConfiguration);

        return new Client(template(factory), factory);

    }

    private static Client createRedisson() {
        Config config = new Config();
        SingleServerConfig c = config.useSingleServer();

        c.setAddress("redis://" + HOST + ":" + PORT);
        c.setPassword(AUTH);
        c.setConnectionPoolSize(POOL_SIZE);
        c.setConnectionMinimumIdleSize(POOL_SIZE);
        RedissonConnectionFactory factory = new RedissonConnectionFactory(config);
        return new Client(template(factory), factory);
    }

    public static RedisTemplate<String, Object> template(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        JdkSerializationRedisSerializer serializer = new JdkSerializationRedisSerializer();
        template.setKeySerializer(stringRedisSerializer);
        template.setHashValueSerializer(stringRedisSerializer);
        template.setValueSerializer(serializer);
        template.setHashKeySerializer(serializer);
        template.setConnectionFactory(factory);
        return template;
    }

    protected ValueOperations<String, Object> v() {
        return redisTemplate.opsForValue();
    }

    protected RedisTemplate<String, Object> t() {
        return redisTemplate;
    }

    protected ListOperations<String, Object> l() {
        return redisTemplate.opsForList();
    }

    protected ZSetOperations<String,Object> z(){
        return redisTemplate.opsForZSet();
    }
    
    static class Client {
        RedisTemplate<String, Object> t;
        RedisConnectionFactory factory;

        public Client(RedisTemplate<String, Object> t, RedisConnectionFactory factory) {
            this.t = t;
            this.factory = factory;
            init();
        }

        void init() {
            try {
                ((InitializingBean) factory).afterPropertiesSet();
            } catch (Exception e) {
                throw new IllegalStateException("failed to init " + factory.getClass().getName(), e);
            }
            t.afterPropertiesSet();
        }
    }
    
    protected byte[] serialize(String s){
        return serializer.serialize(s);
    }

    protected boolean set(byte[] key, byte[] value) {
        return t().execute((RedisCallback<Boolean>) con -> con.set(key, value));
    }

    protected byte[] get(byte[] key) {
        return t().execute((RedisCallback<byte[]>) con -> con.get(key));
    }

    protected Long bitCount(byte[] key, long start, long end) {
        return t().execute((RedisCallback<Long>) cn -> cn.bitCount(key, start, end));
    }

    protected Long bitCount(byte[] key) {
        return t().execute((RedisCallback<Long>) cn -> cn.bitCount(key, 0, -1L));
    }

    protected boolean isRedisson() {
        return t().getConnectionFactory().getClass().getName().toLowerCase().contains("redisson");
    }

    protected boolean isJedis() {
        return t().getConnectionFactory().getClass().getName().toLowerCase().contains("jedis");
    }
}
