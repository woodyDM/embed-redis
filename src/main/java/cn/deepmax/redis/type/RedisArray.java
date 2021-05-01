package cn.deepmax.redis.type;

import cn.deepmax.redis.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class RedisArray extends AbstractRedisType {

    private final List<RedisType> types = new ArrayList<>();

    public static final RedisArray NIL = new RedisArray() {
        @Override
        public boolean isNil() {
            return true;
        }
    };
    
    public RedisArray() {
        super(Type.ARRAY);
    }
    
    @Override
    public boolean isArray() {
        return true;
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
        return types.get(i);
    }

    @Override
    public String respContent() {
        StringBuilder sb = new StringBuilder();
        sb.append("*");
        if (isNil()) {
            sb.append("-1");
        } else {
            sb.append(children().size());
        }
        sb.append(Constants.EOL);
        if (!isNil()) {
            for (RedisType type : children()) {
                sb.append(type.respContent());
            }
        }
        return sb.toString();
    }
    
}
