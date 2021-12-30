package cn.deepmax.redis.base;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisEngineHolder;
import cn.deepmax.redis.core.DefaultRedisEngine;
import cn.deepmax.redis.core.NettyClient;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.After;
import org.junit.Before;

import java.time.LocalDateTime;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author wudi
 * @date 2021/12/16
 */
public class BaseEngineTest extends BaseTest {

    public static final String AUTH = "123456";
    protected DefaultRedisEngine e;
    protected RedisEngine old;

    @Override
    String auth() {
        return AUTH;
    }

    @Before
    public void setUp() {
        e = DefaultRedisEngine.defaultEngine();
        e.setTimeProvider(timeProvider);
        e.setConfiguration(new RedisConfiguration(6379, AUTH));
        old = RedisEngineHolder.instance();
        RedisEngineHolder.set(e);
    }

    @Override
    protected void mockTime(LocalDateTime time) {
        Objects.requireNonNull(time);
        timeProvider.time = time;
    }

    @After
    public void tearDown() throws Exception {
        RedisEngineHolder.set(old);
    }

    @Override
    public RedisEngine engine() {
        return e;
    }

    public Client noAuthClient() {
        return new NettyClient(new EmbeddedChannel());
    }

    public Client embeddedClient() {
        NettyClient client = new NettyClient(new EmbeddedChannel());
        RedisMessage msg = engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("auth"))
                .append(FullBulkValueRedisMessage.ofString(AUTH))
                .build(), client);

        assertTrue(msg instanceof SimpleStringRedisMessage);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");
        return client;
    }

    protected void del(String k) {
        engine().execute(ListRedisMessage.ofString(String.format("del %s", k)), embeddedClient());
    }

    protected void set(String k, String v) {
        engine().execute(ListRedisMessage.ofString(String.format("set %s %s", k, v)), embeddedClient());
    }

    protected void rpush(String k, String v) {
        engine().execute(ListRedisMessage.ofString(String.format("rpush %s %s", k, v)), embeddedClient());
    }

}
