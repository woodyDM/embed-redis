package cn.deepmax.redis.engine;

import io.netty.util.AttributeKey;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class NettyAuthManager implements AuthManager, NettyRedisClientHelper {
    private String auth;

    private static final AttributeKey<String> AUTH_KEY = AttributeKey.newInstance("AUTH");

    @Override
    public void setAuth(String auth) {
        this.auth = auth;
    }

    @Override
    public boolean tryAuth(String auth, Redis.Client client) {
        if (Objects.equals(this.auth, auth)) {
            channel(client).attr(AUTH_KEY).set("OK");
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean alreadyAuth(Redis.Client client) {
        return channel(client).hasAttr(AUTH_KEY);
    }

    @Override
    public boolean needAuth() {
        return auth != null && auth.length() > 0;
    }
    
}