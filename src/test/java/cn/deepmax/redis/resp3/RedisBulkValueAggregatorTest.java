package cn.deepmax.redis.resp3;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CodecException;
import org.junit.Before;
import org.junit.Test;

import static cn.deepmax.redis.resp3.RedisCodecTestUtil.byteBufOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class RedisBulkValueAggregatorTest  extends BaseRedisResp3Test {


    @Before
    public void setup() throws Exception {
        channel = newChannel();
    }

    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new RedisResp3Decoder(),
                new RedisBulkValueAggregator());
    }


    @Test
    public void shouldDecodeBlogString() {
        assertFalse(channel.writeInbound(byteBufOf("$13")));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("hello \r\nworld")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        assertMsg((FullBulkValueRedisMessage msg) -> {
            assertEquals(msg.type(), RedisMessageType.BLOG_STRING);
            assertEquals(msg.str(), "hello \r\nworld");
        });
    }

    @Test
    public void shouldDecodeBlogEmptyString() {
        assertFalse(channel.writeInbound(byteBufOf("$0")));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        assertMsg((FullBulkValueRedisMessage msg) -> {
            assertEquals(msg.type(), RedisMessageType.BLOG_STRING);
            assertEquals(msg.str(), "");
        });
    }

    @Test
    public void shouldDecodeBlogStringError() {
        expectedException.expect(CodecException.class);
        expectedException.expectMessage("length: -1 (expected: >= 0)");

        assertFalse(channel.writeInbound(byteBufOf("$-1")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

    }


    @Test
    public void shouldDecodeVerbatimString() {
        assertFalse(channel.writeInbound(byteBufOf("=13")));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("hello \r\nworld")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        assertMsg((FullBulkValueRedisMessage msg) -> {
            assertEquals(msg.type(), RedisMessageType.VERBATIM_STRING);
            assertEquals(msg.str(), "hello \r\nworld");
        });
    }

    @Test
    public void shouldDecodeBlogError() {
        assertFalse(channel.writeInbound(byteBufOf("!13")));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertFalse(channel.writeInbound(byteBufOf("hello \r\nworld")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        assertMsg((FullBulkValueRedisMessage msg) -> {
            assertEquals(msg.type(), RedisMessageType.BLOG_ERROR);
            assertEquals(msg.str(), "hello \r\nworld");
        });
    }

    @Test
    public void shouldDecodeBlogEmptyError() {
        assertFalse(channel.writeInbound(byteBufOf("!0")));
        assertFalse(channel.writeInbound(byteBufOf("\r\n")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        assertMsg((FullBulkValueRedisMessage msg) -> {
            assertEquals(msg.type(), RedisMessageType.BLOG_ERROR);
            assertEquals(msg.str(), "");
        });
    }


}