package cn.deepmax.redis.type;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class RedisString extends AbstractSimpleRedisType<String> {
    
    public RedisString(String v) {
        super(Type.STRING, v == null ? "" : v);
    }

    @Override
    public boolean isString() {
        return true;
    }

    @Override
    public String str() {
        return value;
    }

    @Override
    protected String respPre() {
        return "+";
    }
}
