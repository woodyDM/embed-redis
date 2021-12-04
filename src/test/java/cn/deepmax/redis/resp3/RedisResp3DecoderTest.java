package cn.deepmax.redis.resp3;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.redis.*;
import io.netty.util.ReferenceCountUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static cn.deepmax.redis.resp3.RedisCodecTestUtil.byteBufOf;
import static cn.deepmax.redis.resp3.RedisCodecTestUtil.bytesOf;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/3
 */
public class RedisResp3DecoderTest extends BaseRedisResp3Test {

    @Before
    public void setup() throws Exception {
        channel = newChannel();
    }

    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new RedisResp3Decoder());
    }

    @Test
    public void shouldDecodeBlogString() {
        assertFalse(channel.writeInbound(byteBufOf("$13")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        BulkValueHeaderRedisMessage msg = channel.readInbound();
        assertEquals(msg.getType(), RedisMessageType.BLOG_STRING);
        ReferenceCountUtil.release(msg);

        assertTrue(channel.writeInbound(byteBufOf("hello \r\nworld")));

        DefaultBulkStringRedisContent msg2 = channel.readInbound();
        assertEquals(msg2.content().toString(StandardCharsets.UTF_8), "hello \r\nworld");
        ReferenceCountUtil.release(msg2);

        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        DefaultLastBulkStringRedisContent msg3 = channel.readInbound();
        assertEquals(msg3.content().toString(StandardCharsets.UTF_8), "");
        ReferenceCountUtil.release(msg3);
    }


    @Test
    public void shouldDecodeBlogEmptyString() {
        assertFalse(channel.writeInbound(byteBufOf("$0")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n\r\n")));

        BulkValueHeaderRedisMessage msg = channel.readInbound();
        assertEquals(msg.getType(), RedisMessageType.BLOG_STRING);
        assertEquals(msg.getLength(), 0);
        ReferenceCountUtil.release(msg);

        LastBulkStringRedisContent msg1 = channel.readInbound();
        assertEquals(msg1.content().toString(StandardCharsets.UTF_8), "");
        assertThat(msg1, sameInstance(LastBulkStringRedisContent.EMPTY_LAST_CONTENT));
        ReferenceCountUtil.release(msg1);
    }

    @Test
    public void shouldDecodeBlogString2() {
        assertFalse(channel.writeInbound(byteBufOf("$14")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        BulkValueHeaderRedisMessage msg = channel.readInbound();
        assertEquals(msg.getType(), RedisMessageType.BLOG_STRING);
        ReferenceCountUtil.release(msg);

        assertTrue(channel.writeInbound(byteBufOf("hello \r\nworl")));

        DefaultBulkStringRedisContent msg2 = channel.readInbound();
        assertEquals(msg2.content().toString(StandardCharsets.UTF_8), "hello \r\nworl");
        ReferenceCountUtil.release(msg2);

        assertTrue(channel.writeInbound(byteBufOf("11\r\n")));

        DefaultLastBulkStringRedisContent msg3 = channel.readInbound();
        assertEquals(msg3.content().toString(StandardCharsets.UTF_8), "11");
        ReferenceCountUtil.release(msg3);
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
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        BulkValueHeaderRedisMessage msg = channel.readInbound();
        assertEquals(msg.getType(), RedisMessageType.VERBATIM_STRING);
        ReferenceCountUtil.release(msg);

        assertTrue(channel.writeInbound(byteBufOf("hello \r\nworld")));

        DefaultBulkStringRedisContent msg2 = channel.readInbound();
        assertEquals(msg2.content().toString(StandardCharsets.UTF_8), "hello \r\nworld");
        ReferenceCountUtil.release(msg2);

        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        DefaultLastBulkStringRedisContent msg3 = channel.readInbound();
        assertEquals(msg3.content().toString(StandardCharsets.UTF_8), "");
        ReferenceCountUtil.release(msg3);
    }

    @Test
    public void shouldDecodeBlogError() {
        assertFalse(channel.writeInbound(byteBufOf("!13")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        BulkValueHeaderRedisMessage msg = channel.readInbound();
        assertEquals(msg.getType(), RedisMessageType.BLOG_ERROR);
        ReferenceCountUtil.release(msg);

        assertTrue(channel.writeInbound(byteBufOf("hello \r\nworld")));

        DefaultBulkStringRedisContent msg2 = channel.readInbound();
        assertEquals(msg2.content().toString(StandardCharsets.UTF_8), "hello \r\nworld");
        ReferenceCountUtil.release(msg2);

        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        DefaultLastBulkStringRedisContent msg3 = channel.readInbound();
        assertEquals(msg3.content().toString(StandardCharsets.UTF_8), "");
        ReferenceCountUtil.release(msg3);
    }

    @Test
    public void shouldDecodeArray() {
        assertTrue(channel.writeInbound(byteBufOf("*2\r\n")));

        assertMsg((AggRedisTypeHeaderMessage msg) -> {
            assertEquals(msg.getType(), RedisMessageType.AGG_ARRAY);
            assertEquals(msg.length(), 2);
        });

        assertTrue(channel.writeInbound(byteBufOf("$2\r\nOk\r\n")));

        assertMsg((BulkValueHeaderRedisMessage msg) -> {
            assertEquals(msg.getType(), RedisMessageType.BLOG_STRING);
            assertEquals(msg.getLength(), 2);
        });

        assertMsg((BulkStringRedisContent msg) -> {
            assertEquals(msg.content().toString(StandardCharsets.UTF_8), "Ok");
        });

        assertTrue(channel.writeInbound(byteBufOf("!5\r\nError\r\n=6\r\nmkd:51\r\n")));

        assertMsg((BulkValueHeaderRedisMessage msg) -> {
            assertEquals(msg.getType(), RedisMessageType.BLOG_ERROR);
            assertEquals(msg.getLength(), 5);
        });

        assertMsg((BulkStringRedisContent msg) -> {
            assertEquals(msg.content().toString(StandardCharsets.UTF_8), "Error");
        });

        assertMsg((BulkValueHeaderRedisMessage msg) -> {
            assertEquals(msg.getType(), RedisMessageType.VERBATIM_STRING);
            assertEquals(msg.getLength(), 6);
        });

        assertMsg((BulkStringRedisContent msg) -> {
            assertEquals(msg.content().toString(StandardCharsets.UTF_8), "mkd:51");
        });
    }

    @Test
    public void shouldDecodeMapError() {
        expectedException.expect(DecoderException.class);
        expectedException.expectMessage("invalid length for map or attribute type");

        channel.writeInbound(byteBufOf("%3\r\n"));

    }

}