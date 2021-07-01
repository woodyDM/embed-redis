package cn.deepmax.redis.api;

/**
 * @author wudi
 * @date 2021/5/10
 */
public interface AuthManager {

    boolean needAuth();

    void setAuth(String auth);

    boolean tryAuth(String auth, Redis.Client client);

    boolean alreadyAuth(Redis.Client client);

}
