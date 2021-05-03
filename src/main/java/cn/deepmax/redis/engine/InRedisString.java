package cn.deepmax.redis.engine;

import cn.deepmax.redis.infra.TimeProvider;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class InRedisString implements TtlObject {

    private byte[] s;

    private LocalDateTime expire;

    public InRedisString(byte[] v) {
        this(v, null);
    }

    public InRedisString(byte[] v, LocalDateTime expire) {
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
