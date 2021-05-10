package cn.deepmax.redis.engine;

import io.netty.channel.Channel;

/**
 * @author wudi
 * @date 2021/5/10
 */
public interface AuthManager {

    boolean needAuth();

    void setAuth(String auth);

    boolean tryAuth(String auth, Channel channel);

    boolean alreadyAuth(Channel channel);

}
