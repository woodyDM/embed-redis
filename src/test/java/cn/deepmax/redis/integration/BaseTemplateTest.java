package cn.deepmax.redis.integration;

import cn.deepmax.redis.RedisServer;
import cn.deepmax.redis.engine.RedisConfiguration;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.redisson.spring.data.connection.RedissonConnectionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public abstract class BaseTemplateTest {

    public static final String AUTH = "123456";
    public static final String HOST = "localhost";
    public static final int PORT = 6380;
    public static final Client[] ts = new Client[3];
    protected static RedisServer server;

    protected RedisTemplate<String, Object> redisTemplate;

    static {
        server = new RedisServer(new RedisConfiguration(PORT, AUTH));
        server.start();

        ts[0] = createJedis();
        ts[1] = createLettuce();
        ts[2] = createRedisson();
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

        JedisConnectionFactory factory = new JedisConnectionFactory(config);
        return new Client(template(factory), factory);
    }

    private static Client createLettuce() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setDatabase(0);
        config.setHostName(HOST);
        config.setPort(PORT);
        config.setPassword(AUTH);

        LettuceClientConfiguration lettuceClientConfiguration = LettuceClientConfiguration.builder()
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
        RedissonConnectionFactory factory = new RedissonConnectionFactory(config);
        return new Client(template(factory), factory);
    }

    public static RedisTemplate<String, Object> template(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> temp = new RedisTemplate<>();
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        JdkSerializationRedisSerializer serializer = new JdkSerializationRedisSerializer();
        temp.setKeySerializer(stringRedisSerializer);
        temp.setHashValueSerializer(stringRedisSerializer);
        temp.setValueSerializer(serializer);
        temp.setHashKeySerializer(serializer);

        temp.setConnectionFactory(factory);
        return temp;
    }

    protected ValueOperations<String, Object> v() {
        return redisTemplate.opsForValue();
    }

    protected RedisTemplate<String, Object> t() {
        return redisTemplate;
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
                throw new IllegalStateException("failed to init " + factory.getClass().getName());
            }
            t.afterPropertiesSet();
        }
    }
}
