/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package cn.deepmax.redis.resp3;

import cn.deepmax.redis.type.CallbackRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.redis.*;
import io.netty.util.internal.UnstableApi;

import java.util.List;

/**
 * Encodes {@link RedisMessage} into bytes following
 * <a href="https://github.com/antirez/RESP3/blob/master/spec.md">RESP3 </a>.
 */
@UnstableApi
public class RedisResp3Encoder extends MessageToMessageEncoder<RedisMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RedisMessage msg, List<Object> out) throws Exception {
        try {
            writeRedisMessage(ctx.alloc(), msg, out);
        } catch (CodecException e) {
            throw e;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

    /**
     * if else 的顺序不能随便调整
     *
     * @param allocator
     * @param msg
     * @param out
     */
    private void writeRedisMessage(ByteBufAllocator allocator, RedisMessage msg, List<Object> out) {
        if (msg instanceof CallbackRedisMessage) {
            msg = ((CallbackRedisMessage) msg).unwrap();
        }
        if (msg instanceof InlineCommandRedisMessage) {
            writeInlineCommandMessage(allocator, (InlineCommandRedisMessage) msg, out);
        } else if (msg instanceof SimpleStringRedisMessage) {
            writeSimpleStringMessage(allocator, (SimpleStringRedisMessage) msg, out);
        } else if (msg instanceof ErrorRedisMessage) {
            writeErrorMessage(allocator, (ErrorRedisMessage) msg, out);
        } else if (msg instanceof IntegerRedisMessage) {
            writeIntegerMessage(allocator, (IntegerRedisMessage) msg, out);
        } else if (msg instanceof FullBulkValueRedisMessage) {
            FullBulkValueRedisMessage m = (FullBulkValueRedisMessage) msg;
            writeFullBulkValueMessage(allocator, m, m.type(), out);
        } else if (msg instanceof FullBulkStringRedisMessage) {
            writeFullBulkStringMessage(allocator, (FullBulkStringRedisMessage) msg, out);
        } else if (msg instanceof BulkStringRedisContent) {
            writeBulkStringContent(allocator, (BulkStringRedisContent) msg, out);
        } else if (msg instanceof BulkValueHeaderRedisMessage) {
            writeBulkValueHeader(allocator, (BulkValueHeaderRedisMessage) msg, out);
        } else if (msg instanceof BulkStringHeaderRedisMessage) {
            writeBulkStringHeader(allocator, (BulkStringHeaderRedisMessage) msg, out);
        } else if (msg instanceof NullRedisMessage) {
            writeNullMessage(allocator, out);
        } else if (msg instanceof DoubleRedisMessage) {
            DoubleRedisMessage fmsg = (DoubleRedisMessage) msg;
            writeString(allocator, RedisMessageType.DOUBLE, fmsg.content(), out);
        } else if (msg instanceof BooleanRedisMessage) {
            writeString(allocator, RedisMessageType.BOOLEAN, ((BooleanRedisMessage) msg).content(), out);
        } else if (msg instanceof BigNumberRedisMessage) {
            writeString(allocator, RedisMessageType.BIG_NUMBER, ((BigNumberRedisMessage) msg).content(), out);
        } else if (msg instanceof ListRedisMessage) {
            writeArrayMessage(allocator, RedisMessageType.AGG_ARRAY, (ArrayRedisMessage) msg, out);
        } else if (msg instanceof SetRedisMessage) {
            writeArrayMessage(allocator, RedisMessageType.AGG_SET, (ArrayRedisMessage) msg, out);
        } else if (msg instanceof MapRedisMessage) {
            writeMappedMessage(allocator, RedisMessageType.AGG_MAP, (MapRedisMessage) msg, out);
        } else if (msg instanceof AttributeRedisMessage) {
            writeMappedMessage(allocator, RedisMessageType.AGG_ATTRIBUTE, (AttributeRedisMessage) msg, out);
        } else if (msg instanceof AggRedisTypeHeaderMessage) {
            writeAggHeader(allocator, (AggRedisTypeHeaderMessage) msg, out);
        } else if (msg instanceof ArrayHeaderRedisMessage) {
            writeArrayHeader(allocator, (ArrayHeaderRedisMessage) msg, out);
        } else if (msg instanceof ArrayRedisMessage) {
            writeArrayMessage(allocator, RedisMessageType.AGG_ARRAY, (ArrayRedisMessage) msg, out);
        } else {
            throw new CodecException("unknown message type: " + msg);
        }
    }

    private static void writeInlineCommandMessage(ByteBufAllocator allocator, InlineCommandRedisMessage msg,
                                                  List<Object> out) {
        writeString(allocator, RedisMessageType.INLINE_COMMAND, msg.content(), out);
    }

    private static void writeSimpleStringMessage(ByteBufAllocator allocator, SimpleStringRedisMessage msg,
                                                 List<Object> out) {
        writeString(allocator, RedisMessageType.SIMPLE_STRING, msg.content(), out);
    }

    private static void writeErrorMessage(ByteBufAllocator allocator, ErrorRedisMessage msg, List<Object> out) {
        writeString(allocator, RedisMessageType.SIMPLE_ERROR, msg.content(), out);
    }

    private static void writeString(ByteBufAllocator allocator, RedisMessageType type, String content,
                                    List<Object> out) {
        ByteBuf buf = allocator.ioBuffer(type.length() + ByteBufUtil.utf8MaxBytes(content) +
                Constants.EOL_LENGTH);
        type.writeTo(buf);
        ByteBufUtil.writeUtf8(buf, content);
        buf.writeShort(Constants.EOL_SHORT);
        out.add(buf);
    }

    private static void writeBulkStringContent(ByteBufAllocator allocator, BulkStringRedisContent msg,
                                               List<Object> out) {
        out.add(msg.content().retain());
        if (msg instanceof LastBulkStringRedisContent) {
            out.add(allocator.ioBuffer(Constants.EOL_LENGTH).writeShort(Constants.EOL_SHORT));
        }
    }

    private void writeNullMessage(ByteBufAllocator allocator, List<Object> out) {
        ByteBuf buf = allocator.ioBuffer(RedisMessageType.NULL.length() +
                Constants.EOL_LENGTH);
        RedisMessageType.NULL.writeTo(buf);
        buf.writeShort(Constants.EOL_SHORT);
        out.add(buf);
    }

    private void writeIntegerMessage(ByteBufAllocator allocator, IntegerRedisMessage msg, List<Object> out) {
        ByteBuf buf = allocator.ioBuffer(Constants.TYPE_LENGTH + Constants.LONG_MAX_LENGTH +
                Constants.EOL_LENGTH);
        RedisMessageType.NUMBER.writeTo(buf);
        buf.writeBytes(numberToBytes(msg.value()));
        buf.writeShort(Constants.EOL_SHORT);
        out.add(buf);
    }

    private void writeBulkStringHeader(ByteBufAllocator allocator, BulkStringHeaderRedisMessage msg, List<Object> out) {
        final ByteBuf buf = allocator.ioBuffer(Constants.TYPE_LENGTH +
                (msg.isNull() ? Constants.NULL_LENGTH :
                        Constants.LONG_MAX_LENGTH + Constants.EOL_LENGTH));
        RedisMessageType.BLOG_STRING.writeTo(buf);
        if (msg.isNull()) {
            buf.writeShort(Constants.NULL_SHORT);
        } else {
            buf.writeBytes(numberToBytes(msg.bulkStringLength()));
            buf.writeShort(Constants.EOL_SHORT);
        }
        out.add(buf);
    }

    private void writeBulkValueHeader(ByteBufAllocator allocator, BulkValueHeaderRedisMessage msg, List<Object> out) {
        final ByteBuf buf = allocator.ioBuffer(Constants.TYPE_LENGTH + Constants.LONG_MAX_LENGTH + Constants.EOL_LENGTH);
        msg.getType().writeTo(buf);
        buf.writeBytes(numberToBytes(msg.getLength()));
        buf.writeShort(Constants.EOL_SHORT);
        out.add(buf);
    }

    private void writeFullBulkStringMessage(ByteBufAllocator allocator, FullBulkStringRedisMessage msg,
                                            List<Object> out) {
        if (msg.isNull()) {
            ByteBuf buf = allocator.ioBuffer(Constants.TYPE_LENGTH + Constants.NULL_LENGTH +
                    Constants.EOL_LENGTH);
            RedisMessageType.BLOG_STRING.writeTo(buf);
            buf.writeShort(Constants.NULL_SHORT);
            buf.writeShort(Constants.EOL_SHORT);
            out.add(buf);
        } else {
            writeFullBulkValueMessage(allocator, msg, RedisMessageType.BLOG_STRING, out);
        }
    }

    private void writeFullBulkValueMessage(ByteBufAllocator allocator, FullBulkStringRedisMessage msg, RedisMessageType type,
                                           List<Object> out) {

        ByteBuf headerBuf = allocator.ioBuffer(Constants.TYPE_LENGTH + Constants.LONG_MAX_LENGTH +
                Constants.EOL_LENGTH);
        type.writeTo(headerBuf);
        headerBuf.writeBytes(numberToBytes(msg.content().readableBytes()));
        headerBuf.writeShort(Constants.EOL_SHORT);
        out.add(headerBuf);
        out.add(msg.content().retain());
        out.add(allocator.ioBuffer(Constants.EOL_LENGTH).writeShort(Constants.EOL_SHORT));
    }

    /**
     * Write agg header only without body. Use this if you want to write arrays as streaming.
     */
    private void writeAggHeader(ByteBufAllocator allocator, AggRedisTypeHeaderMessage msg, List<Object> out) {
        writeArrayHeader(allocator, msg.getType(), msg.isNull(), msg.length(), out);
    }

    /**
     * Write array header only without body. Use this if you want to write arrays as streaming.
     */
    private void writeArrayHeader(ByteBufAllocator allocator, ArrayHeaderRedisMessage msg, List<Object> out) {
        writeArrayHeader(allocator, RedisMessageType.AGG_ARRAY, msg.isNull(), msg.length(), out);
    }

    /**
     * Write map message.
     */
    private void writeMappedMessage(ByteBufAllocator allocator, RedisMessageType type, AbstractMapRedisMessage msg, List<Object> out) {
        writeArrayHeader(allocator, type, false, msg.size(), out);
        msg.data().forEach((k, v) -> {
            writeRedisMessage(allocator, k, out);
            writeRedisMessage(allocator, v, out);
        });
    }

    /**
     * Write full constructed array-based message.
     */
    private void writeArrayMessage(ByteBufAllocator allocator, RedisMessageType type, ArrayRedisMessage msg, List<Object> out) {
        if (msg.isNull()) {
            writeArrayHeader(allocator, type, msg.isNull(), Constants.NULL_VALUE, out);
        } else {
            writeArrayHeader(allocator, type, msg.isNull(), msg.children().size(), out);
            for (RedisMessage child : msg.children()) {
                writeRedisMessage(allocator, child, out);
            }
        }
    }

    private void writeArrayHeader(ByteBufAllocator allocator, RedisMessageType type, boolean isNull, long length, List<Object> out) {
        if (isNull) {
            final ByteBuf buf = allocator.ioBuffer(Constants.TYPE_LENGTH + Constants.NULL_LENGTH +
                    Constants.EOL_LENGTH);
            type.writeTo(buf);
            buf.writeShort(Constants.NULL_SHORT);
            buf.writeShort(Constants.EOL_SHORT);
            out.add(buf);
        } else {
            final ByteBuf buf = allocator.ioBuffer(Constants.TYPE_LENGTH + Constants.LONG_MAX_LENGTH +
                    Constants.EOL_LENGTH);
            type.writeTo(buf);
            buf.writeBytes(numberToBytes(length));
            buf.writeShort(Constants.EOL_SHORT);
            out.add(buf);
        }
    }

    private byte[] numberToBytes(long value) {
        return RedisCodecUtil.longToAsciiBytes(value);
    }
}
