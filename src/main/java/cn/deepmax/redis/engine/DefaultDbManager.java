package cn.deepmax.redis.engine;

import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author wudi
 * @date 2021/5/20
 */
public class DefaultDbManager implements DbManager, NettyRedisClientHelper {

    private final int total;
    private final RedisEngine.Db[] dbs;
    private static final AttributeKey<Integer> IDX = AttributeKey.valueOf("DB_INDEX");

    public DefaultDbManager(int total) {
        this.total = total;
        this.dbs = new RedisDatabase[total];
        for (int i = 0; i < total; i++) {
            dbs[i] = new RedisDatabase();
        }
    }

    @Override
    public RedisEngine.Db get(int index) {
        if (index >= 0 && index < total) {
            return dbs[index];
        }
        throw new RedisParamException("db number out of bound");
    }

    @Override
    public void switchTo(Redis.Client client, int index) {
        if (index >= 0 && index < total) {
            channel(client).attr(IDX).set(index);
        } else {
            throw new RedisParamException("db number out of bound");
        }
    }

    @Override
    public int getIndex(Redis.Client client) {
        Attribute<Integer> attr = channel(client).attr(IDX);
        attr.setIfAbsent(0);
        return attr.get();
    }

    @Override
    public int getTotal() {
        return total;
    }

}
