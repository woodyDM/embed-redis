package cn.deepmax.redis.type;

/**
 * composite type should be sent separately
 * @see RedisArray
 * @author wudi
 * @date 2021/5/10
 */
public class CompositeRedisType extends MultiRedisType {

    public CompositeRedisType() {
        super(Type.COMPOSITE);
    }

    @Override
    public boolean isComposite() {
        return true;
    }

    @Override
    public byte[] respContent() {
        throw new UnsupportedOperationException();
    }
}
