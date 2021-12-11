package cn.deepmax.redis.type;

import cn.deepmax.redis.Constants;

/**
 * 
 * @author wudi
 * @date 2021/4/30
 */
public class RedisArray extends MultiRedisType {

    public static final RedisArray NIL = new RedisArray() {
        @Override
        public boolean isNull() {
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
    public byte[] respContent() {
        ByteBuilder sb = new ByteBuilder();
        sb.append("*");
        if (isNull()) {
            sb.append("-1");
        } else {
            sb.append(children().size());
        }
        sb.append(Constants.EOL);
        if (!isNull()) {
            for (RedisType type : children()) {
                sb.append(type.respContent());
            }
        }
        return sb.build();
    }
    
}
