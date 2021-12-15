package cn.deepmax.redis.core;


import cn.deepmax.redis.api.Redis;
import io.netty.channel.Channel;

/**
 * @author wudi
 * @date 2021/6/25
 */
public interface NettyRedisClientHelper {

    default Channel channel(Redis.Client client) {
        if (client.id() instanceof io.netty.channel.Channel) {
            return (Channel) client.id();
        } else {
            throw new IllegalArgumentException("Invalid client type");
        }
    }

}
