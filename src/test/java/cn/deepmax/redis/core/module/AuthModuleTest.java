package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.base.BaseEngineTest;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/17
 */
public class AuthModuleTest extends BaseEngineTest {
    @Test
    public void shouldOK() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("auth 123456"), noAuthClient());

        assertTrue(msg instanceof SimpleStringRedisMessage);
        assertEquals("OK", ((SimpleStringRedisMessage) msg).content());
    }

    @Test
    public void shouldNoAuth() {
        Redis.Client client = noAuthClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("auth 123456"), client);
        RedisMessage msg2 = engine().execute(ListRedisMessage.ofString("get 12"), client);

        assertTrue(msg2 instanceof FullBulkStringRedisMessage);
        assertSame(msg2, FullBulkStringRedisMessage.NULL_INSTANCE);
    }

    @Test
    public void shouldAuthAndGet() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("get 12"), noAuthClient());

        assertTrue(msg instanceof ErrorRedisMessage);
        assertEquals("NOAUTH Authentication required.", ((ErrorRedisMessage) msg).content());
    }

    @Test
    public void shouldErr() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("auth 1234567"), noAuthClient());

        assertTrue(msg instanceof ErrorRedisMessage);
        assertEquals("WRONGPASS invalid username-password pair", ((ErrorRedisMessage) msg).content());
    }
}