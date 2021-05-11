package cn.deepmax.redis.netty;

import cn.deepmax.redis.type.RedisType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * outBound -> encode redisType to bytes
 */
public class RedisEncoder extends MessageToByteEncoder<RedisType> {
    /**
     * encode
     * @param ctx
     * @param msg
     * @param out
     * @throws Exception
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, RedisType msg, ByteBuf out) throws Exception {
        try {
            if (msg.isComposite()) {
                for (RedisType child : msg.children()) {
                    byte[] bytes = child.respContent();
                    out.writeBytes(bytes);
                }
            }else{
                byte[] bytes = msg.respContent();
                out.writeBytes(bytes);  
            }
        } catch (CodecException e) {
            throw e;
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }

}
