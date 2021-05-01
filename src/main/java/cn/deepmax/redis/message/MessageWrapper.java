package cn.deepmax.redis.message;

import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.nio.charset.StandardCharsets;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class MessageWrapper {
    
    private ArrayRedisMessage message;

    public MessageWrapper(ArrayRedisMessage message) {
        this.message = message;
    }

    public int size() {
        return message.children().size();
    }
    public String getAt(int i){
        return ((FullBulkStringRedisMessage) message.children().get(i)).content().toString(StandardCharsets.UTF_8);
    }
    
}
