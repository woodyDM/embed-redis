package cn.deepmax.redis.netty;

import cn.deepmax.redis.type.RedisType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.MessageToByteEncoder;

public class RedisEncoder extends MessageToByteEncoder<RedisType> {
    @Override
    protected void encode(ChannelHandlerContext ctx, RedisType msg, ByteBuf out) throws Exception {

        try {
            byte[] bytes = msg.respContent();
            out.writeBytes(bytes);
        } catch (CodecException e) {
            throw e;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

}
