package cn.deepmax.redis.type;

import cn.deepmax.redis.Constants;

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

    public static RedisBulkString valueOf(String v) {
        return v == null ? NIL : new RedisBulkString(v);
    }

    public RedisBulkString(String v) {
        super(Type.BULK_STRING);
        this.nil = false;
        this.bytes = v.getBytes(StandardCharsets.UTF_8);
    }

    public RedisBulkString(byte[] v) {
        super(Type.BULK_STRING);
        this.nil = false;
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
    public String respContent() {
        StringBuilder sb = new StringBuilder();
        sb.append("$");
        if (nil) {
            sb.append("-1");
        } else {
            sb.append(bytes.length).append(Constants.EOL)
                    .append(new String(bytes, StandardCharsets.UTF_8));
        }
        sb.append(Constants.EOL);
        return sb.toString();
    }
}
