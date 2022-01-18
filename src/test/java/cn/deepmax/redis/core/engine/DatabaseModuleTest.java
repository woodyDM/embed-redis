package cn.deepmax.redis.core.engine;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.base.BaseMemEngineTest;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author wudi
 */
public class DatabaseModuleTest extends BaseMemEngineTest {

    @Test
    public void shouldSelect() {
        Client client = embeddedClient();
        RedisMessage msg2 = engine().execute(ListRedisMessage.ofString("select 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg2).content(), "OK");

        engine().execute(ListRedisMessage.ofString("set 1 that"), client);
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("get 1"), client);

        assertEquals(((FullBulkStringRedisMessage) msg).content().toString(StandardCharsets.UTF_8), "that");
        engine().execute(ListRedisMessage.ofString("select 1"), client);

        RedisMessage msg3 = engine().execute(ListRedisMessage.ofString("get 1  "), client);
        assertSame(msg3, FullBulkStringRedisMessage.NULL_INSTANCE);
    }

}