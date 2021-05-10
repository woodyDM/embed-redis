package cn.deepmax.redis.engine;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class RedisConfiguration {
    private int port = 6379;
    private String auth;

    public RedisConfiguration(int port, String auth) {
        this.port = port;
        this.auth = auth;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }
}
