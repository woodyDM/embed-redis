package cn.deepmax.redis.type;

import cn.deepmax.redis.Constants;

import java.nio.charset.StandardCharsets;

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
    public byte[] respContent() {
        return (respPre() + value + Constants.EOL).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return name() + value;
    }
    
    
}
