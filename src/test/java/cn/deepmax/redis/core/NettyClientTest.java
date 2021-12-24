package cn.deepmax.redis.core;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/24
 */
public class NettyClientTest {

    @Test
    public void shouldQueue() {
        NettyClient client = new NettyClient(new EmbeddedChannel());
        
        assertFalse(client.scripting());
        assertFalse(client.queued());
        
        client.setQueue(false);
        client.setScripting(false);
        assertFalse(client.scripting());
        assertFalse(client.queued());
        
        client.setQueue(true);
        client.setScripting(true);
        assertTrue(client.scripting());
        assertTrue(client.queued());

        client.setQueue(true);
        client.setScripting(true);
        assertTrue(client.scripting());
        assertTrue(client.queued());

        client.setQueue(false);
        client.setScripting(false);
        assertFalse(client.scripting());
        assertFalse(client.queued());

        client.setQueue(false);
        client.setScripting(false);
        assertFalse(client.scripting());
        assertFalse(client.queued());
    }
}