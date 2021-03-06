package cn.deepmax.redis.api;

import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 */
public class RedisServerException extends RuntimeException {

    private final ErrorRedisMessage msg;

    public RedisServerException(ErrorRedisMessage msg) {
        super(msg.content());
        this.msg = msg;
    }

    public RedisServerException(String message) {
        this(new ErrorRedisMessage(message));
    }

    public RedisMessage getMsg() {
        return msg;
    }

}
