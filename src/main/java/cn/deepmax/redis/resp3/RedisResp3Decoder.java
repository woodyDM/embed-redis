package cn.deepmax.redis.resp3;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.redis.*;
import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.List;

/**
 * https://github.com/antirez/RESP3/blob/master/spec.md
 *
 * @author wudi
 */
@Slf4j
public final class RedisResp3Decoder extends ByteToMessageDecoder {
    private final ToPositiveLongProcessor toPositiveLongProcessor = new ToPositiveLongProcessor();
    private final ToPositiveDoubleProcessor toPositiveDoubleProcessor = new ToPositiveDoubleProcessor();
    private final ToPositiveBigNumberProcessor toPositiveBigNumberProcessor = new ToPositiveBigNumberProcessor();
    // current decoding states
    private State state = State.DECODE_TYPE;
    private RedisMessageType type;
    private int remainingBulkLength;

    private static void readEndOfLine(final ByteBuf in) {
        final short delim = in.readShort();
        if (Constants.EOL_SHORT == delim) {
            return;
        }
        final byte[] bytes = RedisCodecUtil.shortToBytes(delim);
        throw new RedisCodecException("delimiter: [" + bytes[0] + "," + bytes[1] + "] (expected: \\r\\n)");
    }

    private static ByteBuf readLine(ByteBuf in) {
        if (!in.isReadable(Constants.EOL_LENGTH)) {
            return null;
        }
        final int lfIndex = in.forEachByte(ByteProcessor.FIND_LF);
        if (lfIndex < 0) {
            return null;
        }
        ByteBuf data = in.readSlice(lfIndex - in.readerIndex() - 1); // `-1` is for CR
        readEndOfLine(in); // validate CR LF
        return data;
    }

    /**
     * @param ctx
     * @param in
     * @param out
     * @throws Exception
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            for (; ; ) {
                switch (state) {
                    case DECODE_TYPE:
                        if (!decodeType(in)) {
                            return;
                        }
                        break;
                    case DECODE_INLINE:
                        if (!decodeInline(in, out)) {
                            return;
                        }
                        break;
                    case DECODE_LENGTH:
                        if (!decodeLength(in, out)) {
                            return;
                        }
                        break;
                    case DECODE_BULK_EOL:
                        if (!decodeBulkValueEndOfLine(in, out)) {
                            return;
                        }
                        break;
                    case DECODE_BULK_CONTENT:
                        if (!decodeBulkStringContent(in, out)) {
                            return;
                        }
                        break;
                    default:
                        throw new RedisCodecException("Unknown state: " + state);
                }
            }
        } catch (RedisCodecException e) {
            resetDecoder();
            throw e;
        } catch (Exception e) {
            resetDecoder();
            throw new RedisCodecException(e);
        }
    }

    private void resetDecoder() {
        state = State.DECODE_TYPE;
        remainingBulkLength = 0;
    }

    private boolean decodeType(ByteBuf in) {
        if (!in.isReadable()) {
            return false;
        }

        type = RedisMessageType.readFrom(in);
        state = type.inline ? State.DECODE_INLINE : State.DECODE_LENGTH;
        return true;
    }

    private boolean decodeInline(ByteBuf in, List<Object> out) {
        ByteBuf lineBytes = readLine(in);
        if (lineBytes == null) {
            return false;
        }
        out.add(newInlineRedisMessage(type, lineBytes));
        resetDecoder();
        return true;
    }

    private boolean decodeLength(ByteBuf in, List<Object> out) throws RedisCodecException {
        ByteBuf lineByteBuf = readLine(in);
        if (lineByteBuf == null) {
            return false;
        }
        final long length = parseRedisNumber(lineByteBuf);
        if (length < Constants.EMPTY_LENGTH_VALUE) {
            throw new RedisCodecException("length: " + length + " (expected: >= " + Constants.EMPTY_LENGTH_VALUE + ")");
        }
        if ((type == RedisMessageType.AGG_ATTRIBUTE || type == RedisMessageType.AGG_MAP) && length % 2 == 1) {
            throw new RedisCodecException("invalid length for map or attribute type ,length: " + length);
        }
        switch (type) {
            case BLOG_STRING:
            case VERBATIM_STRING:
            case BLOG_ERROR:
                if (length > Constants.REDIS_MESSAGE_MAX_LENGTH) {
                    throw new RedisCodecException("length: " + length + " (expected: <= " +
                            Constants.REDIS_MESSAGE_MAX_LENGTH + ")");
                }
                remainingBulkLength = (int) length; // range(int) is already checked.
                return decodeBulkTypes(in, out);
            case AGG_ARRAY:
            case AGG_MAP:
            case AGG_SET:
            case AGG_ATTRIBUTE:
                out.add(new AggRedisTypeHeaderMessage(length, type));
                resetDecoder();
                return true;
            case PUGH_TYPE:
                //todo

            default:
                throw new RedisCodecException("bad type: " + type);
        }
    }

    private boolean decodeBulkTypes(ByteBuf in, List<Object> out) throws RedisCodecException {
        out.add(new BulkValueHeaderRedisMessage(remainingBulkLength, type));
        if (remainingBulkLength == 0) {
            state = State.DECODE_BULK_EOL;
            return decodeBulkValueEndOfLine(in, out);
        } else {
            // expectedBulkLength is always positive.
            state = State.DECODE_BULK_CONTENT;
            return decodeBulkStringContent(in, out);
        }
    }

    // $0\r\n <here> \r\n
    private boolean decodeBulkValueEndOfLine(ByteBuf in, List<Object> out) throws RedisCodecException {
        if (in.readableBytes() < Constants.EOL_LENGTH) {
            return false;
        }
        readEndOfLine(in);
        out.add(LastBulkStringRedisContent.EMPTY_LAST_CONTENT);
        resetDecoder();
        return true;
    }

    // ${expectedBulkLength}\r\n <here> {data...}\r\n
    private boolean decodeBulkStringContent(ByteBuf in, List<Object> out) throws RedisCodecException {
        final int readableBytes = in.readableBytes();
        if (readableBytes == 0 || remainingBulkLength == 0 && readableBytes < Constants.EOL_LENGTH) {
            return false;
        }

        // if this is last frame.
        if (readableBytes >= remainingBulkLength + Constants.EOL_LENGTH) {
            ByteBuf content = in.readSlice(remainingBulkLength);
            readEndOfLine(in);
            // Only call retain after readEndOfLine(...) as the method may throw an exception.
            out.add(new DefaultLastBulkStringRedisContent(content.retain()));
            resetDecoder();
            return true;
        }

        // chunked write.
        int toRead = Math.min(remainingBulkLength, readableBytes);
        remainingBulkLength -= toRead;
        out.add(new DefaultBulkStringRedisContent(in.readSlice(toRead).retain()));
        return true;
    }

    private RedisMessage newInlineRedisMessage(RedisMessageType messageType, ByteBuf content) {
        switch (messageType) {
            case INLINE_COMMAND:
                return new InlineCommandRedisMessage(content.toString(CharsetUtil.UTF_8));
            case SIMPLE_STRING:
                return new SimpleStringRedisMessage(content.toString(CharsetUtil.UTF_8));
            case SIMPLE_ERROR:
                return new ErrorRedisMessage(content.toString(CharsetUtil.UTF_8));
            case NUMBER:
                return new IntegerRedisMessage(parseRedisNumber(content));
            case NULL:
                if (content.readableBytes() != 0) {
                    throw new RedisCodecException("null should have 0 byte to read, readable is: " + content.readableBytes());
                }
                return NullRedisMessage.INSTANCE;
            case DOUBLE:
                return parseRedisFloat(content);
            case BOOLEAN:
                return parseRedisBoolean(content);
            case BIG_NUMBER:
                return parseBigNumber(content);
            default:
                throw new RedisCodecException("bad type: " + messageType);
        }
    }

    private RedisMessage parseBigNumber(ByteBuf byteBuf) {
        final boolean negative = isNegative(byteBuf);
        if (negative) {
            byteBuf.skipBytes(1);
            return new BigNumberRedisMessage(parsePositiveDecimal(byteBuf).negate());
        } else {
            return new BigNumberRedisMessage(parsePositiveDecimal(byteBuf));
        }
    }

    private RedisMessage parseRedisBoolean(ByteBuf byteBuf) {
        int i = byteBuf.readableBytes();
        if (i != Constants.BOOLEAN_LENGTH) {
            throw new RedisCodecException("bad boolean length :" + i);
        }
        byte b = byteBuf.readByte();
        if (b == 't') {
            return BooleanRedisMessage.TRUE;
        } else if (b == 'f') {
            return BooleanRedisMessage.FALSE;
        } else {
            throw new RedisCodecException("bad boolean value :" + b);
        }
    }

    private DoubleRedisMessage parseRedisFloat(ByteBuf byteBuf) {
        final boolean negative = isNegative(byteBuf);
        final int byteOffset = negative ? 1 : 0;
        // double or inf
        if (byteBuf.readableBytes() == Constants.INF_LENGTH + byteOffset) {
            byte[] b = new byte[Constants.INF_LENGTH];
            byteBuf.getBytes(byteOffset, b);
            if (b[0] == 'i' && b[1] == 'n' && b[2] == 'f') {
                byteBuf.skipBytes(Constants.INF_LENGTH + byteOffset);
                return negative ? DoubleRedisMessage.INF_NEG : DoubleRedisMessage.INF;
            }
        }
        if (negative) {
            byteBuf.skipBytes(1);
            return new DoubleRedisMessage(parsePositiveDouble(byteBuf).negate());
        } else {
            return new DoubleRedisMessage(parsePositiveDouble(byteBuf));
        }
    }

    private boolean isNegative(ByteBuf byteBuf) {
        final int readableBytes = byteBuf.readableBytes();
        final boolean negative = readableBytes > 0 && byteBuf.getByte(byteBuf.readerIndex()) == '-';
        final int extraOneByteForNegative = negative ? 1 : 0;
        if (readableBytes <= extraOneByteForNegative) {
            throw new RedisCodecException("no number to parse: " + byteBuf.toString(CharsetUtil.US_ASCII));
        }
        return negative;
    }

    private long parseRedisNumber(ByteBuf byteBuf) {
        final int readableBytes = byteBuf.readableBytes();
        final boolean negative = readableBytes > 0 && byteBuf.getByte(byteBuf.readerIndex()) == '-';
        final int extraOneByteForNegative = negative ? 1 : 0;
        if (readableBytes <= extraOneByteForNegative) {
            throw new RedisCodecException("no number to parse: " + byteBuf.toString(CharsetUtil.US_ASCII));
        }
        if (readableBytes > Constants.POSITIVE_LONG_MAX_LENGTH + extraOneByteForNegative) {
            throw new RedisCodecException("too many characters to be a valid RESP3 Integer: " +
                    byteBuf.toString(CharsetUtil.US_ASCII));
        }
        if (negative) {
            return -parsePositiveNumber(byteBuf.skipBytes(extraOneByteForNegative));
        }
        return parsePositiveNumber(byteBuf);
    }

    private long parsePositiveNumber(ByteBuf byteBuf) {
        toPositiveLongProcessor.reset();
        byteBuf.forEachByte(toPositiveLongProcessor);
        return toPositiveLongProcessor.content();
    }

    private BigDecimal parsePositiveDouble(ByteBuf byteBuf) {
        toPositiveDoubleProcessor.reset();
        byteBuf.forEachByte(toPositiveDoubleProcessor);
        return toPositiveDoubleProcessor.content();
    }

    private BigDecimal parsePositiveDecimal(ByteBuf byteBuf) {
        toPositiveBigNumberProcessor.reset();
        byteBuf.forEachByte(toPositiveBigNumberProcessor);
        return toPositiveBigNumberProcessor.content();
    }

    private enum State {
        DECODE_TYPE,
        DECODE_INLINE,
        DECODE_LENGTH,
        DECODE_BULK_EOL,
        DECODE_BULK_CONTENT,
    }

    private static final class ToPositiveDoubleProcessor implements ByteProcessor {
        StringBuilder sb = new StringBuilder();

        @Override
        public boolean process(byte value) throws Exception {
            if (value >= '0' && value <= '9') {
                sb.append((char) value);
            } else if (value == '.') {
                if (sb.length() == 0) {
                    throw new RedisCodecException("bad double number,just start with . assuming an initial zero is invalid");
                }
                sb.append((char) value);
            } else {
                throw new RedisCodecException("bad byte in double: " + value);
            }
            return true;
        }

        public BigDecimal content() {
            return new BigDecimal(sb.toString());
        }

        public void reset() {
            sb = new StringBuilder();
        }
    }

    private static final class ToPositiveBigNumberProcessor implements ByteProcessor {
        StringBuilder sb = new StringBuilder();

        @Override
        public boolean process(byte value) throws RedisCodecException {
            if (value < '0' || value > '9') {
                throw new RedisCodecException("bad byte in number: " + value);
            }
            sb.append((char) value);
            return true;
        }

        public BigDecimal content() {
            return new BigDecimal(sb.toString());
        }

        public void reset() {
            sb = new StringBuilder();
        }
    }


    private static final class ToPositiveLongProcessor implements ByteProcessor {
        private long result;

        @Override
        public boolean process(byte value) throws RedisCodecException {
            if (value < '0' || value > '9') {
                throw new RedisCodecException("bad byte in number: " + value);
            }
            result = result * 10 + (value - '0');
            return true;
        }

        public long content() {
            return result;
        }

        public void reset() {
            result = 0;
        }
    }

}
