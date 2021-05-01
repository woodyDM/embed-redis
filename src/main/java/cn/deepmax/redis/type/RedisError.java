package cn.deepmax.redis.type;

import cn.deepmax.redis.Constants;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class RedisError extends AbstractSimpleRedisType<String> {
    
    public RedisError(String v) {
        super(Type.ERROR, v == null ? "" : v);
    }

    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String str() {
        return value;
    }

    @Override
    protected String respPre() {
        return "-";
    }
}
