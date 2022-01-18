package cn.deepmax.redis.lua;

/**
 * used for lua redis.call error
 *
 * @author wudi
 */
public class LuaFuncException extends RuntimeException {
    public LuaFuncException(String message) {
        super(message);
    }
}
