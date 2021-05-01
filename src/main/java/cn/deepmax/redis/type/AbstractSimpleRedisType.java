package cn.deepmax.redis.type;

import cn.deepmax.redis.Constants;

/**
 * @author wudi
 * @date 2021/4/30
 */
public abstract class AbstractSimpleRedisType<T> extends AbstractRedisType {
    protected final T value;

    public AbstractSimpleRedisType(Type type, T value) {
        super(type);
        this.value = value;
    }

    protected abstract String respPre();

    @Override
    public String respContent() {
        return respPre() + value + Constants.EOL;
    }

    @Override
    public String toString() {
        return name() + value;
    }
    
    
}
