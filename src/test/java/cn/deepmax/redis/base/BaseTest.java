package cn.deepmax.redis.base;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.NettyClient;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.MockTimeProvider;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author wudi
 * @date 2021/12/16
 */
public abstract class BaseTest {

    public static final LocalDateTime BASE = LocalDateTime.of(2021, 9, 5, 12, 8, 0);
    protected final static MockTimeProvider timeProvider = new MockTimeProvider();

    static {
        timeProvider.time = BASE;
    }

    abstract String auth();

    @Before
    public void setUp() throws Exception {
        engine().scriptFlush();
        engine().dataFlush();
    }

    abstract public RedisEngine engine();

    public Redis.Client noAuthClient() {
        return new NettyClient(new EmbeddedChannel());
    }

    public Redis.Client embeddedClient() {
        NettyClient client = new NettyClient(new EmbeddedChannel());
        RedisMessage msg = engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("auth"))
                .append(FullBulkValueRedisMessage.ofString(auth()))
                .build(), client);

        assertTrue(msg instanceof SimpleStringRedisMessage);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");
        return client;
    }

    protected byte[] bytes(String k) {
        return k.getBytes(StandardCharsets.UTF_8);
    }

    protected void mockTime(LocalDateTime time) {
        timeProvider.time = Objects.requireNonNull(time);
    }

}
