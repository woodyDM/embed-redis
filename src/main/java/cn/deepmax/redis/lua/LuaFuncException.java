package cn.deepmax.redis.lua;

/**
 * used for lua redis.call error
 *
 * @author wudi
 * @date 2021/5/8
 */
public class LuaFuncException extends RuntimeException {
    public LuaFuncException(String message) {
        super(message);
    }
}
