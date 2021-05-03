package cn.deepmax.redis.type;

import cn.deepmax.redis.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class RedisBulkString extends AbstractRedisType {
    private final boolean nil;
    private byte[] bytes;

    public static final RedisBulkString NIL = new RedisBulkString() {
        @Override
        public boolean isNil() {
            return true;
        }
    };

    private RedisBulkString() {
        super(Type.BULK_STRING);
        this.nil = true;
    }

    public static RedisBulkString of(String v) {
        return v == null ? NIL : new RedisBulkString(v);
    }

    public static RedisBulkString of(byte[] v) {
        return v == null ? NIL : new RedisBulkString(v);
    } 
    
    private RedisBulkString(String v) {
        super(Type.BULK_STRING);
        if (v != null) {
            this.nil = false;
            this.bytes = v.getBytes(StandardCharsets.UTF_8);
        }else{
            this.nil = true;
        }
    }

    private RedisBulkString(byte[] v) {
        super(Type.BULK_STRING);
        this.nil = v == null;
        this.bytes = v;
    }

    @Override
    public String toString() {
        return name() + new String(this.bytes, StandardCharsets.UTF_8);
    }

    //todo
    @Override
    public boolean isString() {
        return true;
    }

    @Override
    public String str() {
        return nil ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] bytes() {
        if (isNil()) {
            return null;
        } else {
            return bytes;
        }
    }

    @Override
    public byte[] respContent() {
        ByteBuilder sb = new ByteBuilder();
        sb.append("$");

        if (isNil()) {
            sb.append("-1");
        } else {
            sb.append(bytes.length).append(Constants.EOL)
                    .append(bytes);
        }
        sb.append(Constants.EOL);
        return sb.build();
    }
}
