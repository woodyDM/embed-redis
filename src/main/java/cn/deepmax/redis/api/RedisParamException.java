package cn.deepmax.redis.api;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class RedisParamException extends RuntimeException {
    public static final RedisParamException SYNTAX_ERR = new RedisParamException("ERR syntax error");
    public static final RedisParamException SYNTAX_ERR_NUMBER = new RedisParamException("ERR value is not an integer or out of range");

    public RedisParamException(String message) {
        super(message);
    }

}
