package cn.deepmax.redis.api;

/**
 * @author wudi
 */
public interface AuthManager {

    void clearAuth(Client client);

    boolean needAuth();

    void setAuth(String auth);

    boolean tryAuth(String auth, Client client);

    boolean alreadyAuth(Client client);

}
