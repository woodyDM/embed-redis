package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Redis;
import io.netty.channel.Channel;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/6/25
 */
public class NettyClient implements Redis.Client {
    private final Channel channel;
    private static final int MASK_QUEUE = 1;
    private static final int MASK_SCRIPTING = 1 << 2;
    private static final AttributeKey<Integer> ATT_FLAG = AttributeKey.newInstance("STATUS_FLAG");

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
    public void pub(RedisMessage msg) {
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
    
    @Override
    public boolean queryFlag(int f) {
        return (getFlag() & f) > 0;
    }

    @Override
    public boolean queued() {
        return queryFlag(FLAG_QUEUE);
    }

    @Override
    public void setQueue(boolean queue) {
        setFlag(FLAG_QUEUE,queue);
    }
    
    @Override
    public void setFlag(int f, boolean value) {
        int flag = getFlag();
        int v = value ? (flag | f) : (flag & (~f));
        channel.attr(ATT_FLAG).set(v);
    }



    private int getFlag() {
        Attribute<Integer> attr = channel.attr(ATT_FLAG);
        attr.setIfAbsent(0);
        return attr.get();
    }

}
