package cn.deepmax.redis.api;

import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class RedisServerException extends RuntimeException {
    
    private final ErrorRedisMessage msg;
    
    public RedisServerException(ErrorRedisMessage msg) {
        super(msg.content());
        this.msg = msg;
    }

    public RedisMessage getMsg() {
        return msg;
    }
 
    public RedisServerException(String message) {
        this(new ErrorRedisMessage(message));
    }

}
