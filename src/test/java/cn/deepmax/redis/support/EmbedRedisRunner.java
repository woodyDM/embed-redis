package cn.deepmax.redis.support;

import cn.deepmax.redis.RedisServer;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.core.DefaultRedisEngine;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

import static cn.deepmax.redis.base.TimedTest.TIME_PROVIDER;

@Slf4j
public class EmbedRedisRunner {
    public static final String AUTH = "123456";
    public static final String SERVER_HOST = "localhost";
    public static int MAIN_PORT;
    public static RedisServer server;
    public static DefaultRedisEngine engine;
    //to change this flag for tests.
    public static TestMode MODE = TestMode.LOCAL_REDIS_STANDALONE;


    public synchronized static DefaultRedisEngine start() {
        if (MODE == TestMode.EMBED_ALL) {
            MAIN_PORT = 6381;
        } else {
            MAIN_PORT = 6380;
        }
        return start(MAIN_PORT, AUTH, AUTH, SERVER_HOST);
    }

    private synchronized static DefaultRedisEngine start(int mainPort, String auth, String clusterAuth, String redisHost) {
        RedisConfiguration.Standalone standalone = new RedisConfiguration.Standalone(mainPort, auth);
        RedisConfiguration.Cluster cluster = new RedisConfiguration.Cluster(clusterAuth, Arrays.asList(
                new RedisConfiguration.Node("m1", 6391)
                        .appendSlave(new RedisConfiguration.Node("s1", 6394)),
                new RedisConfiguration.Node("m2", 6392)
                        .appendSlave(new RedisConfiguration.Node("s2", 6395)),
                new RedisConfiguration.Node("m3", 6393)
                        .appendSlave(new RedisConfiguration.Node("s3", 6396))
        ));
        RedisConfiguration config = new RedisConfiguration(redisHost, standalone, cluster);
        return start(config);
    }

    private synchronized static DefaultRedisEngine start(RedisConfiguration config) {
        if (server != null) {
            log.warn("EmbedRedisRunner called start more than once!");
            return engine;
        }
        engine = DefaultRedisEngine.defaultEngine();
        engine.setTimeProvider(TIME_PROVIDER);
        engine.setConfiguration(config);
        TIME_PROVIDER.reset();
        server = new RedisServer(engine, config);
        if (isEmbededRedis()) {
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.stop();
            }));
        }
        return engine;
    }

    public static boolean isEmbededRedis() {
        return MODE == TestMode.EMBED_ALL;
    }

    public static boolean needCluster() {
        return MODE != TestMode.LOCAL_REDIS_STANDALONE;
    }

    public enum TestMode {
        EMBED_ALL,
        LOCAL_REDIS_STANDALONE,
        LOCAL_REDIS_ALL
    }

}
