package cn.deepmax.redis.base;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.NettyClient;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public interface EngineTest extends ByteHelper {

    RedisEngine engine();

    String auth();

    /* ------------- helper methods -------------*/
    default ExpectedEvents listen(String key) {
        return listen(Collections.singletonList(key));
    }

    default ExpectedEvents listen(List<String> keys) {
        List<Key> list = keys.stream().map(this::bytes)
                .map(Key::new)
                .collect(Collectors.toList());
        ExpectedEvents listener = new ExpectedEvents();
        engine().getDbManager().addListener(embeddedClient(), list, listener);
        return listener;
    }

    default Client noAuthClient() {
        return new NettyClient(new EmbeddedChannel());
    }

    default Client embeddedClient() {
        NettyClient client = new NettyClient(new EmbeddedChannel());
        RedisMessage msg = engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("auth"))
                .append(FullBulkValueRedisMessage.ofString(auth()))
                .build(), client);

        assertTrue(msg instanceof SimpleStringRedisMessage);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");
        return client;
    }


    default void del(String k) {
        engine().execute(ListRedisMessage.ofString(String.format("del %s", k)), embeddedClient());
    }

    default void set(String k, String v) {
        engine().execute(ListRedisMessage.ofString(String.format("set %s %s", k, v)), embeddedClient());
    }

    default void rpush(String k, String v) {
        engine().execute(ListRedisMessage.ofString(String.format("rpush %s %s", k, v)), embeddedClient());
    }

    class ExpectedEvents implements DbManager.KeyEventListener {
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

}
