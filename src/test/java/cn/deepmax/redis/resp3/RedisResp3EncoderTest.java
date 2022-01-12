/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package cn.deepmax.redis.resp3;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static cn.deepmax.redis.resp3.RedisCodecTestUtil.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

/**
 * Verifies the correct functionality of the {@link RedisResp3Encoder} of RESP3.
 * @see RedisMessageType
 */
public class RedisResp3EncoderTest {

    private EmbeddedChannel channel;

    @Before
    public void setup() throws Exception {
        channel = new EmbeddedChannel(new RedisResp3Encoder());
    }

    @After
    public void teardown() throws Exception {
        assertFalse(channel.finish());
    }

    @Test
    public void shouldEncodeNullCommand() {
        RedisMessage msg = NullRedisMessage.INSTANCE;

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf("_\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeTrueBooleanCommand() {
        RedisMessage msg = BooleanRedisMessage.TRUE;

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf("#t\r\n")));
        written.release();
    }
    @Test
    public void shouldEncodeFalseBooleanCommand() {
        RedisMessage msg = BooleanRedisMessage.FALSE;

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf("#f\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeNormalDoubleCommand() {
        RedisMessage msg = new DoubleRedisMessage(new BigDecimal("1.34"));

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf(",1.34\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeNormalDoubleCommand2() {
        RedisMessage msg = new DoubleRedisMessage(new BigDecimal("-0.344"));

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf(",-0.344\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeNormalDoubleCommand3() {
        RedisMessage msg = new DoubleRedisMessage(new BigDecimal("-10000000000.234"));

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf(",-10000000000.234\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeInfDoubleCommand() {
        RedisMessage msg = DoubleRedisMessage.INF;

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf(",inf\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeInfDoubleCommand2() {
        RedisMessage msg = DoubleRedisMessage.INF_NEG;

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf(",-inf\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeBulkStringContent() {
        RedisMessage header = new BulkValueHeaderRedisMessage(16,RedisMessageType.BLOG_STRING);
        RedisMessage body1 = new DefaultBulkStringRedisContent(byteBufOf("bulk\nstr").retain());
        RedisMessage body2 = new DefaultLastBulkStringRedisContent(byteBufOf("ing\ntest").retain());

        assertThat(channel.writeOutbound(header), is(true));
        assertThat(channel.writeOutbound(body1), is(true));
        assertThat(channel.writeOutbound(body2), is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("$16\r\nbulk\nstring\ntest\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeFullBulkString() {
        ByteBuf bulkString = byteBufOf("bulk\nstring\ntest").retain();
        int length = bulkString.readableBytes();
        RedisMessage msg = new FullBulkValueRedisMessage(bulkString,RedisMessageType.BLOG_STRING);

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("$" + length + "\r\nbulk\nstring\ntest\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeBlogErrorContent() {
        RedisMessage header = new BulkValueHeaderRedisMessage(16,RedisMessageType.BLOG_ERROR);
        RedisMessage body1 = new DefaultBulkStringRedisContent(byteBufOf("bulk\nerr").retain());
        RedisMessage body2 = new DefaultLastBulkStringRedisContent(byteBufOf("ing\ntest").retain());

        assertThat(channel.writeOutbound(header), is(true));
        assertThat(channel.writeOutbound(body1), is(true));
        assertThat(channel.writeOutbound(body2), is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("!16\r\nbulk\nerring\ntest\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeFullBlobError() {
        ByteBuf bulkString = byteBufOf("bulk\nerror\ntest\r\nContent").retain();
        int length = bulkString.readableBytes();
        RedisMessage msg = new FullBulkValueRedisMessage(bulkString,RedisMessageType.BLOG_ERROR);

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("!" + length + "\r\nbulk\nerror\ntest\r\nContent\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeVerbatimContent() {
        RedisMessage header = new BulkValueHeaderRedisMessage(16,RedisMessageType.VERBATIM_STRING);
        RedisMessage body1 = new DefaultBulkStringRedisContent(byteBufOf("bulk\nverb").retain());
        RedisMessage body2 = new DefaultLastBulkStringRedisContent(byteBufOf("ng\ntest").retain());

        assertThat(channel.writeOutbound(header), is(true));
        assertThat(channel.writeOutbound(body1), is(true));
        assertThat(channel.writeOutbound(body2), is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("=16\r\nbulk\nverbng\ntest\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeFullVerbatim() {
        ByteBuf bulkString = byteBufOf("bulk\nverbatim\ntest\r\nContent").retain();
        int length = bulkString.readableBytes();
        RedisMessage msg = new FullBulkValueRedisMessage(bulkString,RedisMessageType.VERBATIM_STRING);

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("=" + length + "\r\nbulk\nverbatim\ntest\r\nContent\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeBigNumber() {
        RedisMessage msg = new BigNumberRedisMessage(new BigDecimal("2384902375982374590283754902735982347"));

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf("(2384902375982374590283754902735982347\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeBigNumberNeg() {
        RedisMessage msg = new BigNumberRedisMessage(new BigDecimal("-2384902375982374590283754902735982347"));

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(bytesOf("(-2384902375982374590283754902735982347\r\n")));
        written.release();
    }

    @Test
    public void shouldEncodeSimpleArray() {
        List<RedisMessage> children = new ArrayList<RedisMessage>();
        children.add(new SimpleStringRedisMessage("你好😊"));
        children.add(new FullBulkStringRedisMessage(byteBufOf("bar").retain()));
        children.add(NullRedisMessage.INSTANCE);
        children.add(BooleanRedisMessage.FALSE);
        RedisMessage msg = new ListRedisMessage(children);

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("*4\r\n+你好😊\r\n$3\r\nbar\r\n_\r\n#f\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeSimpleMap() {
        List<RedisMessage> children = new ArrayList<RedisMessage>();
        children.add(new SimpleStringRedisMessage("你好😊"));
        children.add(new FullBulkStringRedisMessage(byteBufOf("bar").retain()));
        children.add(NullRedisMessage.INSTANCE);
        children.add(BooleanRedisMessage.FALSE);
        RedisMessage msg = new MapRedisMessage(children);

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("%2\r\n+你好😊\r\n$3\r\nbar\r\n_\r\n#f\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeEmptySet() {
        List<RedisMessage> children = new ArrayList<RedisMessage>();

        RedisMessage msg = new SetRedisMessage(children);

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        assertThat(bytesOf(written), is(equalTo(bytesOf("~0\r\n"))));
        written.release();
    }

    @Test
    public void shouldEncodeNestAggTypes() {
        List<RedisMessage> children1 = new ArrayList<RedisMessage>();
        children1.add(new SimpleStringRedisMessage("你好😊"));
        children1.add(new FullBulkStringRedisMessage(byteBufOf("bar").retain()));
        children1.add(NullRedisMessage.INSTANCE);
        children1.add(BooleanRedisMessage.FALSE);
        RedisMessage c1 = new MapRedisMessage(children1);


        List<RedisMessage> children2 = new ArrayList<RedisMessage>();
        children2.add(new BigNumberRedisMessage(new BigDecimal("3435345")));
        children2.add(new FullBulkValueRedisMessage(byteBufOf("bar").retain(),RedisMessageType.VERBATIM_STRING));
        RedisMessage c2 = new SetRedisMessage(children2);

        List<RedisMessage> children3 = new ArrayList<RedisMessage>();
        children3.add(new ErrorRedisMessage("err"));
        children3.add(new FullBulkValueRedisMessage(byteBufOf("bar").retain(),RedisMessageType.BLOG_ERROR));
        children3.add(new ListRedisMessage(Collections.singletonList(BooleanRedisMessage.TRUE)));
        RedisMessage c3 = new ListRedisMessage(children3);

        List<RedisMessage> all = new ArrayList<RedisMessage>();
        all.add(c1);
        all.add(c2);
        all.add(c3);
        all.add(BooleanRedisMessage.FALSE);
        RedisMessage msg = new AttributeRedisMessage(all);

        boolean result = channel.writeOutbound(msg);
        assertThat(result, is(true));

        ByteBuf written = readAll(channel);
        String s1 = "%2\r\n+你好😊\r\n$3\r\nbar\r\n_\r\n#f\r\n";
        String s2 = "~2\r\n(3435345\r\n=3\r\nbar\r\n";
        String s3 = "*3\r\n-err\r\n!3\r\nbar\r\n*1\r\n#t\r\n";
        String s4 = "#f\r\n";

        String exp = String.format("|2\r\n%s%s%s%s", s1, s2, s3, s4);
        assertThat(bytesOf(written), is(equalTo(bytesOf(exp))));
        written.release();
    }
}
