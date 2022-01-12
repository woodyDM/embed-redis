package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2022/1/12
 */
public class ConnectionModuleTemplateTest extends BasePureTemplateTest {
    public ConnectionModuleTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldEcho() {
        byte[] e = t().execute((RedisCallback<byte[]>) con -> con.echo(bytes("你👋")));

        assertArrayEquals(e, bytes("你👋"));
    }

    @Test
    public void shouldSetAndGetClientName() {
        if (isRedisson()) {
            return; //java.lang.UnsupportedOperationException
        }
        if (isCluster()) {
            return;
        }
        try (RedisConnection con = t().getConnectionFactory().getConnection()) {
            String name = con.getClientName();
            assertNull(name);
            con.setClientName(bytes("client-test"));

            name = con.getClientName();
            assertEquals(name, "client-test");
        }
    }
}
