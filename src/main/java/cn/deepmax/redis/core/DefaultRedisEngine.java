package cn.deepmax.redis.core;

import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.module.*;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author wudi
 */
@Slf4j
public class DefaultRedisEngine implements RedisEngine {

    private final DefaultRedisExecutor executor = new DefaultRedisExecutor();
    private NettyAuthManager authManager = new NettyAuthManager();
    private NettyAuthManager clusterAuthManager = new NettyAuthManager();
    private final DefaultCommandManager commandManager = new DefaultCommandManager();
    private PubsubManager pubsubManager = new DefaultPubsub();
    private TransactionManager transactionManager = new DefaultTransactionManager(this);
    private final DbManager dbManager = new DefaultDbManager(this, 16);
    protected TimeProvider timeProvider = new DefaultTimeProvider();
    private RedisConfiguration configuration;
    private final List<Flushable> flushList = new ArrayList<>();

    public static DefaultRedisEngine defaultEngine() {
        DefaultRedisEngine e = new DefaultRedisEngine();
        e.loadDefaultModules();
        return e;
    }

    private void loadDefaultModules() {
        loadModule(new StringModule());
        loadModule(new BitMapModule());
        loadModule(new ConnectionModule());
        loadModule(new KeyModule());
        loadModule(new ScriptingModule());
        loadModule(new PubsubModule());
        loadModule(new TransactionModule());
        loadModule(new ListModule());
        loadModule(new SortedSetModule());
        loadModule(new HashModule());
        loadModule(new SetModule());
        loadModule(new ClusterModule());
    }

    @Override
    public Statistic statistic() {
        return executor.statistic();
    }

    @Override
    public void loadModule(Module module) {
        this.commandManager.load(module);
        if (module instanceof Flushable) {
            flushList.add((Flushable) module);
        }
    }

    @Override
    public RedisConfiguration configuration() {
        return configuration;
    }

    @Override
    public void setConfiguration(RedisConfiguration configuration) {
        this.configuration = configuration;
        this.authManager.setAuth(configuration.getStandalone().getAuth());
        if (configuration.getCluster() != null) {
            this.clusterAuthManager.setAuth(configuration.getCluster().getAuth());
        }
    }

    @Override
    public DbManager getDbManager() {
        return dbManager;
    }

    @Override
    public CommandManager commandManager() {
        return commandManager;
    }

    @Override
    public TimeProvider timeProvider() {
        return timeProvider;
    }

    @Override
    public RedisMessage execute(RedisMessage type, Client client) {
        return executor.execute(type, this, client);
    }

    public void setTimeProvider(TimeProvider timeProvider) {
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    @Override
    public AuthManager authManager() {
        return authManager;
    }

    @Override
    public AuthManager clusterAuthManager() {
        return clusterAuthManager;
    }

    @Override
    public PubsubManager pubsub() {
        return pubsubManager;
    }

    @Override
    public TransactionManager transactionManager() {
        return transactionManager;
    }

    @Override
    public void flush() {
        log.debug("Flush all data");
        for (int i = 0; i < dbManager.getTotal(); i++) {
            dbManager.get(i).flush();
        }
        log.debug("Flush all db listener");
        dbManager.flush();
        for (Flushable f : flushList) {
            f.flush();
        }
    }
}
