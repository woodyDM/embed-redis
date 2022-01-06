package cn.deepmax.redis.support;

import cn.deepmax.redis.RedisServer;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.core.DefaultRedisEngine;
import lombok.extern.slf4j.Slf4j;

import static cn.deepmax.redis.base.TimedTest.TIME_PROVIDER;

@Slf4j
public class EmbedRedisRunner {

    public static RedisServer server;
    public static DefaultRedisEngine engine;

    public static boolean isRunning() {
        return server != null;
    }

    public synchronized static DefaultRedisEngine start(int port, String auth) {
        if (server != null) {
            log.warn("EmbedRedisRunner called start more than once!");
            return engine;
        }
        engine = DefaultRedisEngine.defaultEngine();
        engine.setTimeProvider(TIME_PROVIDER);
        TIME_PROVIDER.reset();
        server = new RedisServer(engine, new RedisConfiguration(port, auth));
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
        }));
        return engine;
    }


}