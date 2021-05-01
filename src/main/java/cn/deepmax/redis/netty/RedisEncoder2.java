package cn.deepmax.redis.netty;

import cn.deepmax.redis.type.RedisType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class RedisEncoder2 extends MessageToByteEncoder<RedisType> {
    @Override
    protected void encode(ChannelHandlerContext ctx, RedisType msg, ByteBuf out) throws Exception {

        try {
            String content = msg.respContent();
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

            out.writeBytes(bytes);


        } catch (CodecException e) {
            throw e;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

}
