package cn.deepmax.redis.integration;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisEngineHolder;
import cn.deepmax.redis.core.DefaultRedisEngine;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.EmbedClient;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author wudi
 * @date 2021/12/16
 */
public class BaseEngineTest {

    public static final String AUTH = "123456";

    @BeforeClass
    public static void beforeClass() throws Exception {
        RedisEngineHolder.set(DefaultRedisEngine.instance());
    }

    public static RedisEngine engine() {
        return RedisEngineHolder.instance();
    }

    public static Redis.Client embeddedClient() {
        EmbedClient client = new EmbedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("auth"))
                .append(FullBulkValueRedisMessage.ofString(AUTH))
                .build(), client);

        assertTrue(msg instanceof SimpleStringRedisMessage);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");
        return client;
    }
    
}
