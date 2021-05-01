package cn.deepmax.redis.engine;

import cn.deepmax.redis.infra.TimeProvider;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class InRedisString implements RedisValue{
    
    private byte[] s;
    private TimeProvider provider;
    private LocalDateTime expire;

    public InRedisString(byte[] v, TimeProvider provider ) {
        this(v, provider, null);
    }
    
    public InRedisString(byte[] v, TimeProvider provider, LocalDateTime expire) {
        this.s = v;
        this.provider = provider;
        this.expire = expire;
    }

    public byte[] getS() {
        return s;
    }

    @Override
    public boolean expired() {
        return expire!=null && expire.isBefore(provider.now());
    }

    @Override
    public long ttl() {
        return 0;
    }

    @Override
    public void setTtl(long ttl) {

    }
}
