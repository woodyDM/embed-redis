package cn.deepmax.redis.engine;

/**
 * @author wudi
 * @date 2021/5/20
 */
public interface DbManager {

    default RedisEngine.Db get(Redis.Client client) {
        return get(getIndex(client));
    }

    RedisEngine.Db get(int index);

    int getIndex(Redis.Client client);

    void switchTo(Redis.Client client, int index);

    int getTotal();
}
