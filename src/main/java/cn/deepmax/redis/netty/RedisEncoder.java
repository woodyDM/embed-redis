package cn.deepmax.redis.netty;

import cn.deepmax.redis.type.RedisType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class RedisEncoder extends ChannelOutboundHandlerAdapter  {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        RedisType type = (RedisType) msg;
        try {
            String content = type.respContent();
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            ByteBuf buf = ctx.alloc().ioBuffer(bytes.length);
            buf.writeBytes(bytes);

            ctx.writeAndFlush(buf);

        } catch (CodecException e) {
            throw e;
        } catch (Exception e) {
            throw new CodecException(e);
        }

    }



}
