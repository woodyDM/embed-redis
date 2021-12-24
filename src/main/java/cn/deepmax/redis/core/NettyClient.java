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
    public boolean scripting() {
        return (getFlag() & MASK_SCRIPTING) > 0;
    }

    @Override
    public boolean queued() {
        return (getFlag() & MASK_QUEUE) > 0;
    }

    @Override
    public void setQueue(boolean queue) {
        int flag = getFlag();
        int f = queue ? (flag | MASK_QUEUE) : (flag & (~MASK_QUEUE));
        channel.attr(ATT_FLAG).set(f);
    }

    @Override
    public void setScripting(boolean scripting) {
        int flag = getFlag();
        int f = scripting ? (flag | MASK_SCRIPTING) : (flag & (~MASK_SCRIPTING));
        channel.attr(ATT_FLAG).set(f);
    }

    private int getFlag() {
        Attribute<Integer> attr = channel.attr(ATT_FLAG);
        attr.setIfAbsent(0);
        return attr.get();
    }

}
