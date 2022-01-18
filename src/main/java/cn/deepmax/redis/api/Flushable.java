package cn.deepmax.redis.api;

/**
 * @author wudi
 */
public interface Flushable extends java.io.Flushable {

    @Override
    void flush();

}
