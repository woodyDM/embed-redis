package cn.deepmax.redis.resp3;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.InlineCommandRedisMessage;

/**
 *
 */
public enum RedisMessageType {
    /**
     * {@link InlineCommandRedisMessage}
     */
    INLINE_COMMAND(' ', true),
    /**
     * $<length>\r\n<bytes>\r\n
     *
     * v2 {@link io.netty.handler.codec.redis.BulkStringHeaderRedisMessage}
     * v2 {@link io.netty.handler.codec.redis.BulkStringRedisContent}
     * v2 {@link io.netty.handler.codec.redis.FullBulkStringRedisMessage}
     * v3 {@link BulkValueHeaderRedisMessage}
     * v3 {@link io.netty.handler.codec.redis.BulkStringRedisContent}
     * v3 {@link FullBulkValueRedisMessage}
     *
     */
    BLOG_STRING('$', false),
    /**
     * +<string>\r\n
     * {@link io.netty.handler.codec.redis.SimpleStringRedisMessage}
     */
    SIMPLE_STRING('+', true),
    /**
     * -<string>\r\n    ex:-ERR this is the error description\r\n
     * {@link io.netty.handler.codec.redis.ErrorRedisMessage}
     */
    SIMPLE_ERROR('-', true),
    /**
     * :<number>\r\n
     * {@link io.netty.handler.codec.redis.IntegerRedisMessage}
     */
    NUMBER(':', true),
    /**
     * _\r\n
     * only v3: {@link NullRedisMessage}
     */
    NULL('_', true),
    /**
     * ,<floating-point-number>\r\n          ",inf\r\n"   ",-inf\r\n"
     * only v3: {@link FloatingNumberRedisMessage}
     */
    DOUBLE(',', true),
    /**
     * #t\r\n        #f\r\n
     * only v3: {@link BooleanRedisMessage}
     */
    BOOLEAN('#', true),
    /**
     * !<length>\r\n<bytes>\r\n 
     * only v3
     * v3 {@link BulkValueHeaderRedisMessage}
     * v3 {@link io.netty.handler.codec.redis.BulkStringRedisContent}
     * v3 {@link FullBulkValueRedisMessage}
     */
    BLOG_ERROR('!', false),
    /**
     * =<length>\r\n<bytes>\r\n 
     * only v3
     * v3 {@link BulkValueHeaderRedisMessage}
     * v3 {@link io.netty.handler.codec.redis.BulkStringRedisContent}
     * v3 {@link FullBulkValueRedisMessage}
     */
    VERBATIM_STRING('=', false),
    /**
     * (<big number>\r\n
     * only v3: {@link BigNumberRedisMessage}
     */
    BIG_NUMBER('(', true),

    //Aggregate data types 
    //The format for every aggregate type in RESP3 is always the same:
    //<aggregate-type-char><numelements><CR><LF> ... numelements other types ...
    /**
     * v2 {@link io.netty.handler.codec.redis.ArrayHeaderRedisMessage}
     * v2 {@link io.netty.handler.codec.redis.ArrayRedisMessage}
     * v3 {@link AggRedisTypeHeaderMessage}
     * v3 {@link ListRedisMessage}
     */
    AGG_ARRAY('*', false),
    /**
     * Moreover the number of following elements must be even
     * only v3
     * v3 {@link AggRedisTypeHeaderMessage}
     * v3 {@link ListRedisMessage}
     */
    AGG_MAP('%', false),
    /**
     * only v3
     * v3 {@link AggRedisTypeHeaderMessage}
     * v3 {@link SetRedisMessage}
     */
    AGG_SET('~', false),
    /**
     * |
     * only v3
     * v3 {@link AggRedisTypeHeaderMessage}
     * v3 {@link AttributeRedisMessage}
     */
    AGG_ATTRIBUTE('|', false),
    /**
     * only v3
     * todo
     */
    PUGH_TYPE('>', false);


//    STREAM(' ', false);    // ignored not SUPPORTED

    public final byte value;
    public final boolean inline;

    RedisMessageType(char value, boolean inline) {
        this.value = (byte) value;
        this.inline = inline;
    }

    public int length() {
        return dummy() ? 0 : 1;
    }

    private boolean dummy() {
        return value == ' ';
    }

    /**
     * Write the message type's prefix to the given buffer.
     */
    public void writeTo(ByteBuf out) {
        if (dummy()) {
            return;
        }
        out.writeByte(value);
    }

    /**
     *
     */
    public static RedisMessageType readFrom(ByteBuf in) {
        final int initialIndex = in.readerIndex();
        byte b = in.readByte();
        RedisMessageType type = of(b);
        if (type == INLINE_COMMAND) {
            in.readerIndex(initialIndex);
        }
        return type;
    }
    
    public static RedisMessageType of(byte b){
        for (RedisMessageType v : values()) {
            if (b == v.value) {
                return v;
            }
        }
        return INLINE_COMMAND;
    }


}   
