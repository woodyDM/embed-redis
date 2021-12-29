package cn.deepmax.redis.base;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
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
import java.util.*;
import java.util.stream.Collectors;

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
        engine().flush();
    }

    abstract public RedisEngine engine();

    public Client noAuthClient() {
        return new NettyClient(new EmbeddedChannel());
    }

    public Client embeddedClient() {
        NettyClient client = new NettyClient(new EmbeddedChannel());
        RedisMessage msg = engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("auth"))
                .append(FullBulkValueRedisMessage.ofString(auth()))
                .build(), client);

        assertTrue(msg instanceof SimpleStringRedisMessage);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");
        return client;
    }

    protected ExpectedEvents listen(String key) {
        return listen(Collections.singletonList(key));
    }

    protected ExpectedEvents listen(List<String> keys) {
        List<Key> list = keys.stream().map(this::bytes)
                .map(Key::new)
                .collect(Collectors.toList());
        ExpectedEvents listener = new ExpectedEvents();
        engine().getDbManager().addListener(embeddedClient(), list, listener);
        return listener;
    }

    public static class ExpectedEvents implements DbManager.KeyEventListener {
        public List<DbManager.KeyEvent> events = new ArrayList<>();
        public int triggerTimes = 0;
        public List<DbManager.KeyEvent> filter(String key) {
            return events.stream().filter(f -> Arrays.equals(f.getContent(), (key.getBytes(StandardCharsets.UTF_8))))
                    .collect(Collectors.toList());
        }

        @Override
        public void accept(List<DbManager.KeyEvent> modified, DbManager.KeyEventListener listener) {
            triggerTimes++;
            events.addAll(modified);
        }
    }

    protected byte[] bytes(String k) {
        return k.getBytes(StandardCharsets.UTF_8);
    }

    protected void mockTime(LocalDateTime time) {
        timeProvider.time = Objects.requireNonNull(time);
    }

}
