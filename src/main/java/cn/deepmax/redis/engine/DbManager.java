package cn.deepmax.redis.engine;

import io.netty.channel.Channel;

/**
 * @author wudi
 * @date 2021/5/20
 */
public interface DbManager {
    
    default RedisEngine.Db get(Channel channel) {
        return get(getIndex(channel));
    }

    RedisEngine.Db get(int index);

    int getIndex(Channel channel);

    void switchTo(Channel channel, int index);

    int getTotal();
}
