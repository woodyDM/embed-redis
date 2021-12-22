package cn.deepmax.redis.core;

import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisServerException;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author wudi
 * @date 2021/5/20
 */
public class DefaultDbManager implements DbManager {

    private static final AttributeKey<Integer> IDX = AttributeKey.valueOf("DB_INDEX");
    private final int total;
    private final RedisEngine.Db[] dbs;

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
        throw new RedisServerException("db number out of bound");
    }

    @Override
    public void switchTo(Redis.Client client, int index) {
        if (index >= 0 && index < total) {
            client.channel().attr(IDX).set(index);
        } else {
            throw new RedisServerException("db number out of bound");
        }
    }

    @Override
    public int getIndex(Redis.Client client) {
        Attribute<Integer> attr = client.channel().attr(IDX);
        attr.setIfAbsent(0);
        return attr.get();
    }

    @Override
    public int getTotal() {
        return total;
    }

}
