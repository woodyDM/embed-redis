package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/6/25
 */
public class NettyClient implements Redis.Client {
    private final Channel channel;

    public NettyClient(ChannelHandlerContext ctx) {
        this.channel = ctx.channel();
    }

    @Override
    public Object id() {
        return channel;
    }

    @Override
    public void send(RedisType msg) {
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