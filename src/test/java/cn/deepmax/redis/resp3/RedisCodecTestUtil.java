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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.ssl.ReferenceCountedOpenSslEngine;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;

public final class RedisCodecTestUtil {

    private RedisCodecTestUtil() {
    }

    public static void release(List<RedisMessage> msg){
        for (RedisMessage m : msg) {
            ReferenceCountUtil.release(m);
        }
    }

    public static List<RedisMessage> readAllMessage(Channel channel) {
        List<RedisMessage> r = new ArrayList<>();
        EmbeddedChannel ch = (EmbeddedChannel) channel;
        RedisMessage read;
        while ((read = ch.readOutbound()) != null) {
            r.add(read);
        }
        return r;
    }

    public static ByteBuf readAll(EmbeddedChannel channel) {
        ByteBuf buf = Unpooled.buffer();
        ByteBuf read;
        while ((read = channel.readOutbound()) != null) {
            buf.writeBytes(read);
            read.release();
        }
        return buf;
    }

    static byte[] bytesOf(long value) {
        return bytesOf(Long.toString(value));
    }

    static byte[] bytesOf(String s) {
        return s.getBytes(CharsetUtil.UTF_8);
    }

    static byte[] bytesOf(ByteBuf buf) {
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        return data;
    }

    static String stringOf(ByteBuf buf) {
        return new String(bytesOf(buf));
    }

    static ByteBuf byteBufOf(String s) {
        return byteBufOf(bytesOf(s));
    }

    static ByteBuf byteBufOf(byte[] data) {
        return Unpooled.wrappedBuffer(data);
    }
}
