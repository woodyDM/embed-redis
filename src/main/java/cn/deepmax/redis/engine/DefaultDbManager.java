package cn.deepmax.redis.engine;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author wudi
 * @date 2021/5/20
 */
public class DefaultDbManager implements DbManager {
    
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
    public void switchTo(Channel channel, int index) {
        if (index >= 0 && index < total) {
            channel.attr(IDX).set(index);
        } else {
            throw new RedisParamException("db number out of bound");
        }
    }

    @Override
    public int getIndex(Channel channel) {
        Attribute<Integer> attr = channel.attr(IDX);
        attr.setIfAbsent(0);
        Integer idx = attr.get();
        return idx;
    }

    @Override
    public int getTotal() {
        return total;
    }
}
