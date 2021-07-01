package cn.deepmax.redis.type;

import cn.deepmax.redis.api.RedisParamException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wudi
 * @date 2021/4/30
 */
public abstract class MultiRedisType extends AbstractRedisType {

    private final List<RedisType> types = new ArrayList<>();

    public MultiRedisType(Type type) {
        super(type);
    }

    @Override
    public void add(RedisType type) {
        this.types.add(type);
    }

    @Override
    public List<RedisType> children() {
        return types;
    }

    @Override
    public RedisType get(int i) {
        if (i >= 0 && i < size()) {
            return types.get(i);
        } else {
            throw new RedisParamException("ERR wrong number of arguments ");
        }
    }

}
