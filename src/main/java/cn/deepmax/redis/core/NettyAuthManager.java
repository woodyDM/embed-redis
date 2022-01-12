package cn.deepmax.redis.core;

import cn.deepmax.redis.api.AuthManager;
import cn.deepmax.redis.api.Client;
import io.netty.util.AttributeKey;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class NettyAuthManager implements AuthManager {
    private static final AttributeKey<String> AUTH_KEY = AttributeKey.newInstance("AUTH");
    private String auth;

    @Override
    public void clearAuth(Client client) {
        client.channel().attr(AUTH_KEY).set(null);
    }

    @Override
    public void setAuth(String auth) {
        this.auth = auth;
    }

    @Override
    public boolean tryAuth(String auth, Client client) {
        if (Objects.equals(this.auth, auth)) {
            client.channel().attr(AUTH_KEY).set("OK");
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean alreadyAuth(Client client) {
        return "OK".equals(client.channel().attr(AUTH_KEY).get());
    }

    @Override
    public boolean needAuth() {
        return auth != null && auth.length() > 0;
    }

}
