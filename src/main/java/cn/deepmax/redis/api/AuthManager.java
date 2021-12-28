package cn.deepmax.redis.api;

/**
 * @author wudi
 * @date 2021/5/10
 */
public interface AuthManager {

    boolean needAuth();

    void setAuth(String auth);

    boolean tryAuth(String auth, Client client);

    boolean alreadyAuth(Client client);

}
