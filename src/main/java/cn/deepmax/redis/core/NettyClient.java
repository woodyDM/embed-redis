package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.utils.MessagePrinter;
import io.netty.channel.Channel;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wudi
 * @date 2021/6/25
 */
public class NettyClient implements Client {
    private final Channel channel;
    private final RedisEngine engine;
    private static final AttributeKey<Integer> ATT_FLAG = AttributeKey.newInstance("STATUS_FLAG");
    private static final AttributeKey<Long> ATT_ID = AttributeKey.newInstance("ID_FLAG");
    private static final AtomicLong id = new AtomicLong(1);

    public NettyClient(RedisEngine engine, Channel channel) {
        this.engine = engine;
        this.channel = channel;
        long thisId = id.getAndIncrement();
        channel.attr(ATT_ID).setIfAbsent(thisId);
    }

    @Override
    public RedisEngine engine() {
        return engine;
    }

    /**
     * the client request node
     *
     * @return empty if standalone
     */
    @Override
    public Optional<RedisConfiguration.Node> node() {
        RedisConfiguration.Cluster cluster = engine.configuration().getCluster();
        if (cluster == null) {
            return Optional.empty();
        }
        SocketAddress add = this.channel.localAddress();
        if (add instanceof InetSocketAddress) {
            InetSocketAddress iadd = (InetSocketAddress) add;
            int port = iadd.getPort();
            return cluster.getAllNodes().stream().filter(n -> n.port == port).findFirst();
        } else {
            throw new IllegalStateException("can't decide node of local address" + add);
        }
    }

    @Override
    public Protocol resp() {
        return Client.Protocol.RESP2;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public Object id() {
        return channel.attr(ATT_ID).get();
    }

    @Override
    public void pub(RedisMessage msg) {
        MessagePrinter.responseStart();
        MessagePrinter.printMessage(msg, queued());
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

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("NettyClient{");
        sb.append("channel=").append(channel);
        sb.append('}');
        return sb.toString();
    }
}
