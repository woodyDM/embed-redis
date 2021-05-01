package cn.deepmax.redis.type;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class RedisInteger extends AbstractSimpleRedisType<Long> {

    public RedisInteger(long value) {
        super(Type.INTEGER, value);
    }

    public RedisInteger(int value) {
        this((long) value);
    }

    @Override
    public boolean isInteger() {
        return true;
    }

    @Override
    public long value() {
        return value;
    }

    @Override
    public String toString() {
        return name() + value;
    }

    @Override
    protected String respPre() {
        return ":";
    }
}
