package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.support.AbstractRedisObject;

/**
 * @author wudi
 * @date 2021/4/30
 */
class RString extends AbstractRedisObject {

    private final byte[] s;

    public RString(TimeProvider timeProvider, byte[] s) {
        super(timeProvider);
        this.s = s;
    }

    public byte[] getS() {
        return s;
    }
}
