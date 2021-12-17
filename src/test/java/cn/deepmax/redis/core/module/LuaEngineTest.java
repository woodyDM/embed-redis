package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.base.BaseEngineTest;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.resp3.RedisMessageType;
import cn.deepmax.redis.utils.SHA1Test;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author wudi
 * @date 2021/12/15
 */
public class LuaEngineTest extends BaseEngineTest {

    @Test
    public void shouldLoad() {
        RedisMessage msg = engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("script"))
                .append(FullBulkValueRedisMessage.ofString("load"))
                .append(FullBulkValueRedisMessage.ofString(SHA1Test.SCRIPT)).build(), embeddedClient());

        assertTrue(msg instanceof FullBulkValueRedisMessage);
        assertEquals(((FullBulkValueRedisMessage) msg).type(), RedisMessageType.BLOG_STRING);
        assertEquals(SHA1Test.SCRIPT_SHA, ((FullBulkValueRedisMessage) msg).str());
    }

    @Test
    public void shouldExist() {
        Redis.Client client = embeddedClient();
        RedisMessage msgD = engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("script"))
                .append(FullBulkValueRedisMessage.ofString("load"))
                .append(FullBulkValueRedisMessage.ofString(SHA1Test.SCRIPT)).build(), client);

        RedisMessage msg = engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("script"))
                .append(FullBulkValueRedisMessage.ofString("exists"))
                .append(FullBulkValueRedisMessage.ofString(SHA1Test.SCRIPT_SHA)).build(), client);


        assertTrue(msg instanceof ListRedisMessage);
        RedisMessage first = ((ListRedisMessage) msg).children().get(0);
        assertTrue(first instanceof IntegerRedisMessage);
        assertEquals(((IntegerRedisMessage) first).value(), 1L);
    }

}