package cn.deepmax.redis;

import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.args.Args;
import cn.deepmax.redis.core.DefaultRedisEngine;
import cn.deepmax.redis.netty.RedisServerHandler;
import cn.deepmax.redis.resp3.RedisAggTypesAggregator;
import cn.deepmax.redis.resp3.RedisBulkValueAggregator;
import cn.deepmax.redis.resp3.RedisResp3Decoder;
import cn.deepmax.redis.resp3.RedisResp3Encoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Embed redis server
 */
@Slf4j
public class RedisServer {

    private EventLoopGroup boss = null;
    private EventLoopGroup workerGroup = null;
    private final List<ChannelFuture> binds = new ArrayList<>();
    private final RedisEngine engine;
    private boolean stoped = false;

    public RedisServer(@NonNull RedisEngine engine, RedisConfiguration configuration) {
        configuration.check();
        this.engine = engine;
        this.engine.setConfiguration(configuration);
    }

    public RedisServer(RedisConfiguration configuration) {
        this(DefaultRedisEngine.defaultEngine(), configuration);
    }

    private static final Args.Flag<String> F_AUTH = Args.Flag.newInstance("a", null, Function.identity());
    private static final Args.Flag<String> F_CLUSTER_AUTH = Args.Flag.newInstance("clusterAuth", null, Function.identity());
    private static final Args.Flag<Integer> F_PORT = Args.Flag.newInstance("p", "6381", Integer::new);
    private static final Args.Flag<String> F_HOST = Args.Flag.newInstance("h", "localhost", Function.identity());

    public static void main(String[] args) {
        new Args().flag(F_AUTH)
                .flag(F_CLUSTER_AUTH)
                .flag(F_HOST)
                .flag(F_PORT).parse(args);

        RedisConfiguration.Standalone standalone = new RedisConfiguration.Standalone(F_PORT.get(), F_AUTH.get());
        RedisConfiguration.Cluster cluster = new RedisConfiguration.Cluster(F_CLUSTER_AUTH.get(), Arrays.asList(
                new RedisConfiguration.Node("master1", 6391)
                        .appendSlave(new RedisConfiguration.Node("slave1", 6394)),
                new RedisConfiguration.Node("master2", 6392)
                        .appendSlave(new RedisConfiguration.Node("slave2", 6395)),
                new RedisConfiguration.Node("master3", 6393)
                        .appendSlave(new RedisConfiguration.Node("slave3", 6396))
        ));
        new RedisServer(new RedisConfiguration(F_HOST.get(), standalone, cluster)).startWithShutdownHook();
    }

    public void startWithShutdownHook() {
        start();
        Thread hookThread = new Thread(this::stop);
        hookThread.setName("Embed-redis shutdownHook");
        Runtime.getRuntime().addShutdownHook(hookThread);
    }

    public void start() {
        ServerBootstrap boot = new ServerBootstrap();

        boss = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(1);
        boot.group(boss, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new RedisResp3Decoder())
                                .addLast(new RedisBulkValueAggregator())
                                .addLast(new RedisAggTypesAggregator())
                                .addLast(new RedisResp3Encoder())
                                .addLast(new RedisServerHandler(engine));
                    }
                });
        //to bind ports
        StringBuilder logText = new StringBuilder();
        logText.append("Redis started ");
        RedisConfiguration configuration = configuration();
        if (configuration.getStandalone() != null) {
            int port = configuration.getStandalone().getPort();
            binds.add(h(boot.bind(port), port));
            logText.append("with standalone port ").append(port).append(" ");
        }
        if (configuration.getCluster() != null) {
            logText.append(", with cluster port: ");
            List<RedisConfiguration.Node> nodes = configuration.getCluster().getAllNodes();
            for (int i = 0; i < nodes.size(); i++) {
                int port = nodes.get(i).port;
                binds.add(h(boot.bind(port), port));
                logText.append(port);
                if (i < nodes.size() - 1) {
                    logText.append(",");
                }
            }
        }
        binds.forEach(ChannelFuture::syncUninterruptibly);
        log.info("{} !", logText.toString());
    }

    private ChannelFuture h(ChannelFuture future, int port) {
        return future.addListener(c -> {
            if (!c.isSuccess()) {
                if (!c.cause().getClass().getName().contains("StacklessClosedChannelException")) {
                    log.error("Embed redis failed to start at port " + port, c.cause());
                }
                this.stop();
            }
        });
    }

    public void stop() {
        if (stoped) {
            return;
        }
        stoped = true;
        for (ChannelFuture ch : binds) {
            try {
                ch.channel().close();
                log.info("Redis server shutdown for address: {}", ch.channel().localAddress());
            } catch (Exception e) {
                log.error("Close error ", e);
            }
        }
        if (boss != null) {
            boss.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        log.info("Redis server shutdown successfully !");
    }

    private RedisConfiguration configuration() {
        return engine.configuration();
    }

}
