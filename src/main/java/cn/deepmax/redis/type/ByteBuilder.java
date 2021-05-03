package cn.deepmax.redis.type;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

class ByteBuilder {

    private final ByteBuf buf = Unpooled.buffer();

    public ByteBuilder append(String v) {
        if (v != null && !v.isEmpty()) {
            buf.writeBytes(v.getBytes(StandardCharsets.UTF_8));

        }
        return this;
    }

    public ByteBuilder append(long v) {
        return append(v + "");
    }

    public ByteBuilder append(byte[] v) {
        buf.writeBytes(v);
        return this;
    }

    public byte[] build() {
        int read = buf.readableBytes();
        byte[] b = new byte[read];
        buf.getBytes(0, b);
        return b;
    }
}
