package cn.deepmax.redis;


import cn.deepmax.redis.api.RedisServerException;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import lombok.NonNull;

/**
 * @author wudi
 * @date 2021/4/29
 */
public class Constants {
    public static final ErrorRedisMessage ERR_NO_AUTH = of("NOAUTH Authentication required.");
    public static final ErrorRedisMessage ERR_TYPE = of("ERR Operation against a key holding the wrong kind of value");
    public static final ErrorRedisMessage ERR_SYNTAX = of("ERR syntax error");
    public static final ErrorRedisMessage ERR_SYNTAX_NUMBER = of("ERR value is not an integer or out of range");
    public static final IntegerRedisMessage INT_ZERO = new IntegerRedisMessage(0);
    public static final IntegerRedisMessage INT_ONE_NEG = new IntegerRedisMessage(-1);
    public static final IntegerRedisMessage INT_ONE = new IntegerRedisMessage(1);
    
    public static final RedisServerException EX_SYNTAX = new RedisServerException(Constants.ERR_SYNTAX);

    private static ErrorRedisMessage of(@NonNull String msg) {
        return new ErrorRedisMessage(msg);
    }

    private Constants() {
    }

}
