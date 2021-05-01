package cn.deepmax.redis.type;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/4/30
 */
public abstract class AbstractRedisType implements RedisType{
    protected Type type;

    public AbstractRedisType(Type type) {
        this.type = Objects.requireNonNull(type);
    }

    @Override
    public Type type() {
        return type;
    }

    protected String name() {
        return "[" + getClass().getSimpleName() + "]";
    }
}
