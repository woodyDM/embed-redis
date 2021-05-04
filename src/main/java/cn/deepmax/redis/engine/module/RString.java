package cn.deepmax.redis.engine.module;

import cn.deepmax.redis.engine.RedisObject;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/4/30
 */
class RString implements RedisObject {

    private byte[] s;

    private LocalDateTime expire;

    public RString(byte[] v) {
        this(v, null);
    }

    public RString(byte[] v, LocalDateTime expire) {
        this.s = v;

        this.expire = expire;
    }

    @Override
    public LocalDateTime expireTime() {
        return expire;
    }

    public byte[] getS() {
        return s;
    }


    @Override
    public long ttl() {
        return 0;
    }

    @Override
    public void setTtl(long ttl) {

    }
}
