package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Redis;
import io.netty.channel.Channel;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/6/25
 */
public class NettyClient implements Redis.Client {
    private final Channel channel;

    public NettyClient(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Redis.Protocol resp() {
        return Redis.Protocol.RESP2;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public Object id() {
        return channel;
    }

    @Override
    public void send(RedisMessage msg) {
        channel.writeAndFlush(msg);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NettyClient that = (NettyClient) o;
        return Objects.equals(channel, that.channel);
    }

    @Override
    public int hashCode() {
        return channel.hashCode();
    }
}
