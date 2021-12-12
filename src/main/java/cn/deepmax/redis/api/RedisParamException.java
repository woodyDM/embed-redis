package cn.deepmax.redis.api;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class RedisParamException extends RuntimeException{
    public RedisParamException(String message) {
        super(message);
    }

    public static final RedisParamException SYNTAX_ERR = new RedisParamException("ERR syntax error");
    
}