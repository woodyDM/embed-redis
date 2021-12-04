package cn.deepmax.redis.resp3;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.Before;
import org.junit.Test;

import static cn.deepmax.redis.resp3.RedisCodecTestUtil.byteBufOf;
import static org.junit.Assert.*;

public class RedisAggTypesAggregatorTest extends BaseRedisResp3Test{

    @Before
    public void setup() throws Exception {
        channel = newChannel();
    }

    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new RedisResp3Decoder(),
                new RedisBulkValueAggregator(),
                new RedisAggTypesAggregator());
    }

    @Test
    public void shouldAggList() {
        assertFalse(channel.writeInbound(byteBufOf("*2\r\n$13")));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("hello \r\nworld")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n+OK\r\n")));

        assertMsg((ListRedisMessage msg) -> {
            assertEquals(msg.children().size(), 2);
            FullBulkValueRedisMessage m1 = (FullBulkValueRedisMessage) msg.children().get(0);
            assertEquals(m1.type(),RedisMessageType.BLOG_STRING);
            assertEquals(m1.toString(),"hello \r\nworld");

            SimpleStringRedisMessage m2 = (SimpleStringRedisMessage) msg.children().get(1);
            assertEquals(m2.content(), "OK");
        });
    }

    @Test
    public void shouldAggNestList() {
        assertFalse(channel.writeInbound(byteBufOf("*2\r\n$13")));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("hello \r\nworld")));
        assertFalse(channel.writeInbound(byteBufOf("\r\n%4\r\n+OK\r\n")));
        assertTrue(channel.writeInbound(byteBufOf("-Err\r\n:45\r\n#f\r\n")));

        assertMsg((ListRedisMessage msg) -> {
            assertEquals(msg.children().size(), 2);
            FullBulkValueRedisMessage m1 = (FullBulkValueRedisMessage) msg.children().get(0);
            assertEquals(m1.type(),RedisMessageType.BLOG_STRING);
            assertEquals(m1.toString(),"hello \r\nworld");

            MapRedisMessage m2 = (MapRedisMessage) msg.children().get(1);
            assertEquals(m2.content().size(), 2);
        });
    }
}