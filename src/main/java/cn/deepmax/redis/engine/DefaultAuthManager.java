package cn.deepmax.redis.engine;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class DefaultAuthManager implements AuthManager {
    private String auth;

    private static final AttributeKey<String> AUTH_KEY = AttributeKey.newInstance("AUTH");

    @Override
    public void setAuth(String auth) {
        this.auth = auth;
    }

    @Override
    public boolean tryAuth(String auth, Channel channel) {
        if (Objects.equals(this.auth, auth)) {
            channel.attr(AUTH_KEY).set("OK");
            return true;
        }else{
            return false;
        }
    }

    @Override
    public boolean alreadyAuth(Channel channel) {
        return channel.hasAttr(AUTH_KEY);
    }

    @Override
    public boolean needAuth() {
        return auth != null && auth.length() > 0;
    }
 
}
