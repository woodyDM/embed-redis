package cn.deepmax.redis.api;

/**
 * @author wudi
 * @date 2021/12/29
 */
public interface Flushable extends java.io.Flushable {
    
    @Override
    void flush();
    
}
