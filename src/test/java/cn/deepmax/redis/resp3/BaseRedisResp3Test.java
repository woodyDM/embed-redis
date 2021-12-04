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
public abstract class BaseRedisResp3Test {
    protected EmbeddedChannel channel;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @After
    public void teardown() throws Exception {
        assertFalse(channel.finish());
    }

    @Test
    public void shouldDecodeTwoSimpleStrings() {
        assertFalse(channel.writeInbound(byteBufOf("+")));
        assertFalse(channel.writeInbound(byteBufOf("O")));
        assertFalse(channel.writeInbound(byteBufOf("K")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n+SEC")));
        assertTrue(channel.writeInbound(byteBufOf("OND\r\n")));

        SimpleStringRedisMessage msg1 = channel.readInbound();
        assertThat(msg1.content(), is("OK"));
        ReferenceCountUtil.release(msg1);

        SimpleStringRedisMessage msg2 = channel.readInbound();
        assertThat(msg2.content(), is("SECOND"));
        ReferenceCountUtil.release(msg2);
    }

    @Test
    public void shouldDecodeError() {
        String content = "ERROR sample message";
        assertFalse(channel.writeInbound(byteBufOf("-")));
        assertFalse(channel.writeInbound(byteBufOf(content)));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n")));

        ErrorRedisMessage msg = channel.readInbound();

        assertThat(msg.content(), is(content));

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeInteger() {
        long value = 1234L;
        byte[] content = bytesOf(value);
        assertFalse(channel.writeInbound(byteBufOf(":")));
        assertFalse(channel.writeInbound(byteBufOf(content)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        IntegerRedisMessage msg = channel.readInbound();

        assertThat(msg.value(), is(value));

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeNull() {
        assertFalse(channel.writeInbound(byteBufOf("_")));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        NullRedisMessage msg = channel.readInbound();

        assertThat(msg, is(NullRedisMessage.INS));
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeNullError() {
        expectedException.expect(DecoderException.class);
        expectedException.expectMessage("null should have 0 byte to read, readable is: 2");
        assertFalse(channel.writeInbound(byteBufOf("_12")));
        //
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));
    }

    @Test
    public void shouldDecodeDouble() {
        byte[] value = bytesOf("10.156");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FloatingNumberRedisMessage msg = channel.readInbound();

        assertThat(msg.getValue(), equalTo(10.156));
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeDouble2() {
        byte[] value = bytesOf("-0.156");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FloatingNumberRedisMessage msg = channel.readInbound();

        assertThat(msg.getValue(), equalTo(-0.156));
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeDouble3() {
        byte[] value = bytesOf("100");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FloatingNumberRedisMessage msg = channel.readInbound();

        assertThat(msg.getValue(), equalTo(100D));
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeDouble4() {
        byte[] value = bytesOf("-200");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FloatingNumberRedisMessage msg = channel.readInbound();

        assertThat(msg.getValue(), equalTo(-200D));
        ReferenceCountUtil.release(msg);
    }


    @Test
    public void shouldDecodeDouble5() {
        byte[] value = bytesOf("inf");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FloatingNumberRedisMessage msg = channel.readInbound();

        assertThat(msg, sameInstance(FloatingNumberRedisMessage.INF));
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeDouble6() {
        byte[] value = bytesOf("-inf");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));

        FloatingNumberRedisMessage msg = channel.readInbound();

        assertThat(msg, sameInstance(FloatingNumberRedisMessage.INF_NEG));
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeDoubleError() {
        expectedException.expect(CodecException.class);
        expectedException.expectMessage("bad double number,just start with . assuming an initial zero is invalid");

        byte[] value = bytesOf(".001");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));
    }

    @Test
    public void shouldDecodeDoubleError2() {
        expectedException.expect(CodecException.class);
        expectedException.expectMessage("bad byte in double: ");

        byte[] value = bytesOf("0.1e10");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n")));
    }

    @Test
    public void shouldDecodeDouble7() {
        byte[] value = bytesOf("10.15");
        byte[] value2 = bytesOf("-0.75");
        assertFalse(channel.writeInbound(byteBufOf(",")));
        assertFalse(channel.writeInbound(byteBufOf(value)));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n,")));

        FloatingNumberRedisMessage msg1 = channel.readInbound();
        assertThat(msg1.getValue(), equalTo(10.15D));
        ReferenceCountUtil.release(msg1);

        assertFalse(channel.writeInbound(byteBufOf(value2)));
        assertTrue(channel.writeInbound(byteBufOf("\r\n,-inf\r\n,inf\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n,99\r\n")));


        FloatingNumberRedisMessage msg2 = channel.readInbound();
        assertThat(msg2.getValue(), equalTo(-0.75D));
        ReferenceCountUtil.release(msg2);

        FloatingNumberRedisMessage msg3 = channel.readInbound();
        assertThat(msg3, sameInstance(FloatingNumberRedisMessage.INF_NEG));
        ReferenceCountUtil.release(msg3);

        FloatingNumberRedisMessage msg4 = channel.readInbound();
        assertThat(msg4, sameInstance(FloatingNumberRedisMessage.INF));
        ReferenceCountUtil.release(msg4);

        FloatingNumberRedisMessage msg5 = channel.readInbound();
        assertThat(msg5.getValue(), equalTo(99D));
        ReferenceCountUtil.release(msg5);
    }

    @Test
    public void shouldDecodeBooleanTrue() {
        assertFalse(channel.writeInbound(byteBufOf("#t")));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n")));

        BooleanRedisMessage msg = channel.readInbound();

        assertThat(msg, sameInstance(BooleanRedisMessage.TRUE));
        assertTrue(msg.value());
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeBooleanFalse() {
        assertFalse(channel.writeInbound(byteBufOf("#f")));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n")));

        BooleanRedisMessage msg = channel.readInbound();

        assertThat(msg, sameInstance(BooleanRedisMessage.FALSE));
        assertFalse(msg.value());
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeBooleanError() {
        expectedException.expect(DecoderException.class);
        expectedException.expectMessage("bad boolean value");

        assertFalse(channel.writeInbound(byteBufOf("#s")));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        //raise exception
        channel.writeInbound(byteBufOf("\n"));
    }

    @Test
    public void shouldDecodeBooleanError2() {
        expectedException.expect(DecoderException.class);
        expectedException.expectMessage("bad boolean length :3");

        assertFalse(channel.writeInbound(byteBufOf("#555")));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        //raise exception
        channel.writeInbound(byteBufOf("\n"));
    }

    @Test
    public void shouldDecodeBooleanError3() {
        expectedException.expect(DecoderException.class);
        expectedException.expectMessage("bad boolean length :0");

        assertFalse(channel.writeInbound(byteBufOf("#")));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        //raise exception
        channel.writeInbound(byteBufOf("\n"));
    }

    @Test
    public void shouldDecodeBigNumber() {
        assertFalse(channel.writeInbound(byteBufOf("(3492890328409238509324850943850943825024385")));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n")));

        BigNumberRedisMessage msg = channel.readInbound();
        assertEquals(msg.getValue(),new BigDecimal("3492890328409238509324850943850943825024385"));
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void shouldDecodeBigNumber2() {
        assertFalse(channel.writeInbound(byteBufOf("(-3492890328409238509324850943850943825024385")));
        assertFalse(channel.writeInbound(byteBufOf("\r")));
        assertTrue(channel.writeInbound(byteBufOf("\n")));

        BigNumberRedisMessage msg = channel.readInbound();
        assertEquals(msg.getValue(),new BigDecimal("-3492890328409238509324850943850943825024385"));
        ReferenceCountUtil.release(msg);
    }

    protected  <T> void assertMsg(Consumer<T> s){
        T msg = channel.readInbound();
        s.accept(msg);
        ReferenceCountUtil.release(msg);
    }
}