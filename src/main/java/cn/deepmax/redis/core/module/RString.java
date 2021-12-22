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
    
    public RString append(byte[] a){
        if (a == null || a.length == 0) {
            return this;
        }
        byte[] c = new byte[s.length + a.length];
        System.arraycopy(s,0,c,0,s.length);
        System.arraycopy(a,0,c,s.length,a.length);
        RString rString = new RString(timeProvider, c);
        rString.expireAt(this.expireTime());
        return rString;
    }
    
    public byte[] getS() {
        return s;
    }
}
