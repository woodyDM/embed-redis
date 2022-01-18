package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.utils.MessagePrinter;
import io.netty.channel.Channel;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wudi
 */
public class NettyClient implements Client {

    private final Channel channel;
    private final RedisEngine engine;
    private static final AttributeKey<Info> ATT_INFO = AttributeKey.newInstance("Client_Info");
    private static final AtomicLong ID = new AtomicLong(1);

    public NettyClient(RedisEngine engine, Channel channel) {
        this.engine = engine;
        this.channel = channel;
        if (!channel.hasAttr(ATT_INFO)) {
            Info info = new Info();
            info.id = ID.getAndIncrement();
            channel.attr(ATT_INFO).set(info);
        }
    }

    protected Info getInfo() {
        return channel().attr(ATT_INFO).get();
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
        return getInfo().protocol;
    }

    /**
     * @param p
     */
    @Override
    public void setProtocol(Protocol p) {
        getInfo().protocol = p;
    }

    @Override
    public Channel channel() {
        return channel;
    }

    /**
     * command should be execute when called .
     * when in Script or Transaction , command should not block to wait
     *
     * @return
     */
    @Override
    public boolean commandInstantExec() {
        return queryFlag(FLAG_QUEUE_EXEC) || queryFlag(FLAG_SCRIPTING);
    }

    @Override
    public long id() {
        return getInfo().id;
    }

    @Override
    public void pub(RedisMessage msg) {
        MessagePrinter.responseStart(this, engine.statistic().incrSend());
        MessagePrinter.printMessage(msg, queued());
        channel.writeAndFlush(msg);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NettyClient that = (NettyClient) o;
        return Objects.equals(id(), that.id());
    }

    @Override
    public int hashCode() {
        return Long.valueOf(id()).hashCode();
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
        getInfo().flag = v;
    }

    private int getFlag() {
        return getInfo().flag;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("NettyClient{");
        sb.append("channel=").append(channel);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public byte[] getName() {
        return getInfo().name;
    }


    @Override
    public void setName(byte[] name) {
        getInfo().name = name;
    }

    static class Info {
        byte[] name;
        int flag = 0;
        long id;
        Protocol protocol = Protocol.RESP2;
    }
}

