package cn.deepmax.redis.base;

import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.support.EmbedRedisRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author wudi
 */
public class BaseRedissonScriptTest {

    static RedissonClient redisson;
    static RedisEngine engine;

    @BeforeClass
    public static void beforeClass() throws Exception {
        engine = EmbedRedisRunner.start();

        Config config = new Config();
        SingleServerConfig c = config.useSingleServer();

        c.setAddress("redis://" + "localhost" + ":" + EmbedRedisRunner.MAIN_PORT);
        c.setPassword(EmbedRedisRunner.AUTH);
        c.setConnectionPoolSize(8);
        c.setConnectionMinimumIdleSize(8);

        redisson = Redisson.create(config);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (redisson != null) {
            redisson.shutdown();
        }
    }

    @Before
    public void setUp() throws Exception {
        engine.flush();
    }

    @Test
    public void shouldSimpleLock() throws InterruptedException {
        RLock lock = redisson.getLock("s-lock");
        boolean ok = false;
        try {
            ok = lock.tryLock(5, 10, TimeUnit.MINUTES);
            System.out.println("some action in lock");
        } finally {
            if (ok) {
                lock.unlock();
            }
        }
    }

    @Test
    public void shouldRealLock() throws InterruptedException {
        AtomicReference<Integer> counter = new AtomicReference<>(0);
        AtomicReference<Exception> allE = new AtomicReference<>();
        int totalThread = 24;
        int perThreadIncr = 30;

        Thread[] threads = new Thread[totalThread];
        for (int i = 0; i < totalThread; i++) {
            Thread t = new Thread(new Runnable() {
                int cur = 0;

                @Override
                public void run() {
                    RLock lock = redisson.getLock("s-lock");
                    while (cur < perThreadIncr) {
                        boolean ok = false;
                        try {
                            try {
                                ok = lock.tryLock(5, 10, TimeUnit.MINUTES);
                            } catch (Exception e) {
                                allE.set(e);
                            }
                            if (ok) {
                                Integer old = counter.get();
                                old = old + 1;
                                counter.set(old);
                                cur++;
                            }
                        } finally {
                            if (ok) {
                                lock.unlock();
                            }
                        }
                    }

                }
            });
            threads[i] = t;
            t.start();
        }
        for (int i = 0; i < totalThread; i++) {
            threads[i].join();
        }
        //assert
        assertNull(allE.get());
        assertEquals(counter.get().intValue(), totalThread * perThreadIncr);
    }

    @Test
    public void shouldBpop() throws InterruptedException {
        RBlockingDeque<Object> queue = redisson.getBlockingDeque("queue");
        Object obj = queue.pollFirst(1, TimeUnit.SECONDS);

        assertNull(obj);
    }
}
