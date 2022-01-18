package cn.deepmax.redis;


import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.RedisCommand;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import lombok.NonNull;

import static cn.deepmax.redis.core.RedisCommand.OK;

/**
 * @author wudi
 */
public class Constants {
    public static final ErrorRedisMessage ERR_NO_AUTH = of("NOAUTH Authentication required.");
    public static final ErrorRedisMessage ERR_TYPE = of("ERR Operation against a key holding the wrong kind of value");
    public static final ErrorRedisMessage ERR_SYNTAX = of("ERR syntax error");
    public static final ErrorRedisMessage ERR_NAN = of("ERR NAN");
    public static final ErrorRedisMessage ERR_IMPL_MISMATCH = of("ERR Embed-redis internal type mismatch!");
    public static final ErrorRedisMessage ERR_NOT_SUPPORT = of("ERR redis 7.X command ,not support");
    public static final ErrorRedisMessage ERR_SYNTAX_NUMBER = of("ERR value is not an integer or out of range");
    public static final ErrorRedisMessage ERR_NO_CLUSTER = of("ERR not cluster mode");
    public static final IntegerRedisMessage INT_ZERO = new IntegerRedisMessage(0);
    public static final IntegerRedisMessage INT_ONE_NEG = new IntegerRedisMessage(-1);
    public static final IntegerRedisMessage INT_ONE = new IntegerRedisMessage(1);
    public static final SimpleStringRedisMessage QUEUED = new SimpleStringRedisMessage("QUEUED");
    public static final SimpleStringRedisMessage RESET = new SimpleStringRedisMessage("RESET");
    public static final RedisServerException EX_SYNTAX = new RedisServerException(Constants.ERR_SYNTAX);

    public static final RedisCommand UNKNOWN_COMMAND = ((type, ctx, engine) -> new ErrorRedisMessage("Embed-redis does not support this command"));
    public static final RedisCommand COMMAND_OK = ((type, ctx, engine) -> OK);

    private static ErrorRedisMessage of(@NonNull String msg) {
        return new ErrorRedisMessage(msg);
    }

    private Constants() {
    }
}
