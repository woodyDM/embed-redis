package cn.deepmax.redis.core;

import cn.deepmax.redis.base.EngineTest;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author wudi
 */
public class NettyClientTest {

    @Test
    public void shouldQueue() {
        EngineTest.MockClient client = new EngineTest.MockClient(DefaultRedisEngine.defaultEngine(), new EmbeddedChannel());
        
        assertFalse(client.queued());
        
        client.setQueue(false);
        assertFalse(client.queued());
        
        client.setQueue(true);
        assertTrue(client.queued());

        client.setQueue(true);
        assertTrue(client.queued());

        client.setQueue(false);
        assertFalse(client.queued());

        client.setQueue(false);
        assertFalse(client.queued());
    }
}