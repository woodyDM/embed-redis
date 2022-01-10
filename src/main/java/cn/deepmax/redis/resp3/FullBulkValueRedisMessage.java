package cn.deepmax.redis.resp3;

import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class FullBulkValueRedisMessage extends FullBulkStringRedisMessage {
    private final RedisMessageType type;

    public FullBulkValueRedisMessage(ByteBuf content, RedisMessageType type) {
        super(content);
        this.type = type;
    }

    public static FullBulkStringRedisMessage ofDouble(Double v) {
        return ofString(NumberUtils.formatDouble(v));
    }

    public static FullBulkStringRedisMessage ofString(String v) {
        if (v == null) {
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        } else if (v.length() == 0) {
            return FullBulkStringRedisMessage.EMPTY_INSTANCE;
        } else {
            ByteBuf content = Unpooled.wrappedBuffer(v.getBytes(StandardCharsets.UTF_8));
            return new FullBulkValueRedisMessage(content, RedisMessageType.BLOG_STRING);
        }
    }

    public static FullBulkStringRedisMessage ofString(byte[] v) {
        if (v == null) {
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        } else if (v.length == 0) {
            return FullBulkStringRedisMessage.EMPTY_INSTANCE;
        } else {
            ByteBuf content = Unpooled.wrappedBuffer(v);
            return new FullBulkValueRedisMessage(content, RedisMessageType.BLOG_STRING);
        }
    }

    public RedisMessageType type() {
        return type;
    }

    public byte[] bytes() {
        int len = content().readableBytes();
        byte[] bs = new byte[len];
        content().getBytes(content().readerIndex(), bs);
        return bs;
    }

    public Key key() {
        return new Key(bytes());
    }

    public String str() {
        return content().toString(StandardCharsets.UTF_8);
    }

    public Long val() {
        return NumberUtils.parse(str());
    }

    @Override
    public String toString() {
        return "[" + content().toString(Charset.defaultCharset()) + "]";
    }

}
