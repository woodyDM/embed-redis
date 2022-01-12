package cn.deepmax.redis.base;

import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.support.EmbedRedisRunner;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.redisson.spring.data.connection.RedissonConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * redis template test support
 */
abstract class BaseTemplateTest implements ByteHelper {
    protected RedisTemplate<String, Object> redisTemplate;
    public static final Logger log = LoggerFactory.getLogger(BaseTemplateTest.class);
    protected static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    protected static final int POOL_SIZE = 4;
    protected static final String AUTH = "123456";
    protected static final String HOST = "localhost";
    protected static final String SERVER_HOST = "localhost";
    protected static final RedisConfiguration config;
    protected static RedisEngine engine;
    protected static final int MAIN_PORT;
    //to change this flag for tests.
    protected static final TestMode MODE = TestMode.EMBED_ALL;

    static {
        if (MODE == TestMode.EMBED_ALL) {
            MAIN_PORT = 6380;
        } else {
            MAIN_PORT = 6379;
        }
        RedisConfiguration.Standalone standalone = new RedisConfiguration.Standalone(MAIN_PORT, AUTH);
        RedisConfiguration.Cluster cluster = new RedisConfiguration.Cluster(AUTH, Arrays.asList(
                new RedisConfiguration.Node("m1", 6391)
                        .appendSlave(new RedisConfiguration.Node("s1", 6394)),
                new RedisConfiguration.Node("m2", 6392)
                        .appendSlave(new RedisConfiguration.Node("s2", 6395)),
                new RedisConfiguration.Node("m3", 6393)
                        .appendSlave(new RedisConfiguration.Node("s3", 6396))
        ));
        config = new RedisConfiguration(SERVER_HOST, standalone, cluster);

        scheduler.submit(() -> System.out.println("warn up"));

        if (isEmbededRedis()) {
            engine = EmbedRedisRunner.start(config);
        }
    }

    enum TestMode {
        EMBED_ALL,
        LOCAL_REDIS_STANDALONE,
        LOCAL_REDIS_ALL
    }

    protected static boolean isEmbededRedis() {
        return MODE == TestMode.EMBED_ALL;
    }

    protected static boolean needCluster() {
        return MODE != TestMode.LOCAL_REDIS_STANDALONE;
    }

    public BaseTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    protected static Client[] initStandalone() {
        Client[] clients = new Client[4];
        log.info("Init with  needCluster  {}", needCluster());
        RedisConfiguration.Standalone standalone = config.getStandalone();
        clients[0] = createRedissonStandalone(HOST, standalone.getPort(), standalone.getAuth());
        clients[1] = createJedisStandalone(HOST, standalone.getPort(), standalone.getAuth());
        clients[2] = createLettuceStandalone(HOST, standalone.getPort(), standalone.getAuth(), ProtocolVersion.RESP2);
        clients[3] = createLettuceStandalone(HOST, standalone.getPort(), standalone.getAuth(), ProtocolVersion.RESP3);
        return clients;
    }

    protected static Client[] initCluster() {
        Client[] clients = new Client[3];
        log.info("Init with  needCluster  {}", needCluster());
        if (needCluster()) {
            RedisConfiguration.Cluster cluster = config.getCluster();
            clients[0] = createRedissonCluster(HOST, cluster);
            clients[1] = createLettuceCluster(HOST, cluster);
            clients[2] = createJedisCluster(HOST, cluster);
        }
        return clients;
    }

    private static Client createJedisCluster(String host, RedisConfiguration.Cluster cluster) {
        RedisClusterConfiguration config = createClusterConfig(host, cluster);

        JedisClientConfiguration jedisClientConfiguration = JedisClientConfiguration.defaultConfiguration();
        jedisClientConfiguration.getPoolConfig().ifPresent(c -> {
            c.setMaxIdle(POOL_SIZE);
            c.setMaxTotal(POOL_SIZE);
            c.setMinIdle(POOL_SIZE);
        });

        JedisConnectionFactory factory = new JedisConnectionFactory(config, jedisClientConfiguration);
        return new Client(template(factory), factory);
    }

    private static List<RedisNode> toConfigNodes(String host, RedisConfiguration.Cluster cluster) {
        List<RedisNode> nodes = new ArrayList<>();
        List<RedisConfiguration.Node> masters = cluster.getMasterNodes();
        for (RedisConfiguration.Node master : masters) {
            nodes.add(RedisNode.newRedisNode()
                    .listeningAt(host, master.port)
                    .withId(master.id).withName(master.name).promotedAs(RedisNode.NodeType.MASTER).build());
            for (RedisConfiguration.Node slave : master.getSlaves()) {
                nodes.add(RedisNode.newRedisNode()
                        .listeningAt(host, slave.port)
                        .slaveOf(master.id)
                        .withId(slave.id).withName(slave.name).promotedAs(RedisNode.NodeType.SLAVE).build());
            }
        }
        return nodes;
    }

    private static Client createLettuceCluster(String host, RedisConfiguration.Cluster cluster) {
        RedisClusterConfiguration config = createClusterConfig(host, cluster);

        LettuceClientConfiguration lettuceClientConfiguration = LettuceClientConfiguration.builder()
                .clientResources(ClientResources.builder()
                        .build())
                .clientOptions(ClientOptions.builder()
                        .protocolVersion(ProtocolVersion.RESP2)
                        .build()).build();

        LettuceConnectionFactory factory = new LettuceConnectionFactory(config, lettuceClientConfiguration);

        return new Client(template(factory), factory);
    }

    private static RedisClusterConfiguration createClusterConfig(String host, RedisConfiguration.Cluster cluster) {
        List<RedisNode> nodes = toConfigNodes(host, cluster);

        RedisClusterConfiguration config = new RedisClusterConfiguration();
        config.setPassword(cluster.getAuth());
        config.setMaxRedirects(5);
        config.setClusterNodes(nodes);
        return config;
    }

    private static Client createRedissonCluster(String host, RedisConfiguration.Cluster cluster) {
        List<String> adds = cluster.getAllNodes().stream()
                .map(n -> "redis://" + host + ":" + n.port)
                .collect(Collectors.toList());

        Config config = new Config();
        ClusterServersConfig c = config.useClusterServers();
        c.setPassword(cluster.getAuth());
        c.setNodeAddresses(adds);

        RedissonConnectionFactory factory = new RedissonConnectionFactory(config);
        return new Client(template(factory), factory);
    }

    protected static Client createJedisStandalone(String host, int port, String auth) {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setDatabase(0);
        config.setHostName(host);
        config.setPort(port);
        config.setPassword(auth);

        JedisClientConfiguration jedisClientConfiguration = JedisClientConfiguration.defaultConfiguration();
        jedisClientConfiguration.getPoolConfig().ifPresent(c -> {
            c.setMaxIdle(POOL_SIZE);
            c.setMaxTotal(POOL_SIZE);
            c.setMinIdle(POOL_SIZE);
        });
        JedisConnectionFactory factory = new JedisConnectionFactory(config, jedisClientConfiguration);
        return new Client(template(factory), factory);
    }

    protected static Client createLettuceStandalone(String host, int port, String auth, ProtocolVersion version) {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setDatabase(0);
        config.setHostName(host);
        config.setPort(port);
        config.setPassword(auth);

        LettuceClientConfiguration lettuceClientConfiguration = LettuceClientConfiguration.builder()
                .clientResources(ClientResources.builder()
                        .build())
                .clientOptions(ClientOptions.builder()
                        .protocolVersion(version)
                        .build()).build();

        LettuceConnectionFactory factory = new LettuceConnectionFactory(config, lettuceClientConfiguration);

        return new Client(template(factory), factory);
    }

    protected static Client createRedissonStandalone(String host, int port, String auth) {
        Config config = new Config();
        SingleServerConfig c = config.useSingleServer();

        c.setAddress("redis://" + host + ":" + port);
        c.setPassword(auth);
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

    protected HashOperations<String,Object,Object> h(){
        return redisTemplate.opsForHash();
    }

    protected RedisTemplate<String, Object> t() {
        return redisTemplate;
    }

    protected ListOperations<String, Object> l() {
        return redisTemplate.opsForList();
    }

    protected ZSetOperations<String, Object> z() {
        return redisTemplate.opsForZSet();
    }

    protected SetOperations<String, Object> s() {
        return redisTemplate.opsForSet();
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

    public byte[] bytes(String k) {
        return k.getBytes(StandardCharsets.UTF_8);
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

    protected boolean isLettuce() {
        return t().getConnectionFactory().getClass().getName().toLowerCase().contains("lettuce");
    }

    protected boolean isJedis() {
        return t().getConnectionFactory().getClass().getName().toLowerCase().contains("jedis");
    }

    protected boolean isCluster() {
        RedisClusterConnection cn = null;
        try {
            cn = t().getConnectionFactory().getClusterConnection();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (cn != null) cn.close();
        }
    }
}
