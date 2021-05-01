package cn.deepmax.redis.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

import java.nio.charset.StandardCharsets;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class Messages {
    
    public static FullBulkStringRedisMessage bulkString(String s){
        if (s == null) {
            return FullBulkStringRedisMessage.NULL_INSTANCE;
        } else if (s.length() == 0) {
            return FullBulkStringRedisMessage.EMPTY_INSTANCE;
        }else{
            ByteBuf buf = Unpooled.wrappedBuffer(s.getBytes(StandardCharsets.UTF_8));
            return new FullBulkStringRedisMessage(buf);
        }
    }
}
