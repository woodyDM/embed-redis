package cn.deepmax.redis.core;

import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.module.*;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * @author wudi
 * @date 2021/4/30
 */
@Slf4j
public class DefaultRedisEngine implements RedisEngine {

    private final RedisExecutor executor = new DefaultRedisExecutor();
    private NettyAuthManager authManager = new NettyAuthManager();
    private final DefaultCommandManager commandManager = new DefaultCommandManager();
    private PubsubManager pubsubManager = new DefaultPubsub();
    private TransactionManager transactionManager = new DefaultTransactionManager(this);
    private final DbManager dbManager = new DefaultDbManager(16);
    protected TimeProvider timeProvider = new DefaultTimeProvider();
    private Runnable scriptFlushAction;
    private RedisConfiguration configuration;

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
        ScriptingModule luaModule = new ScriptingModule();
        loadModule(luaModule);
        scriptFlushAction = luaModule::flush;
        loadModule(new ConnectionModule());
        loadModule(new PubsubModule());
        loadModule(new TransactionModule());
    }

    @Override
    public void loadModule(Module module) {
        this.commandManager.load(module);
    }

    @Override
    public RedisConfiguration configuration() {
        return configuration;
    }

    @Override
    public void setConfiguration(RedisConfiguration configuration) {
        this.configuration = configuration;
        this.authManager.setAuth(configuration.getAuth());
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
    public RedisMessage execute(RedisMessage type, Redis.Client client) {
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
    public PubsubManager pubsub() {
        return pubsubManager;
    }

    @Override
    public void dataFlush() {
        log.debug("Flush all data");
        for (int i = 0; i < dbManager.getTotal(); i++) {
            dbManager.get(i).flush();
        }
    }

    @Override
    public TransactionManager transactionManager() {
        return transactionManager;
    }

    @Override
    public void scriptFlush() {
        if (scriptFlushAction != null) {
            log.debug("Flush all script");
            scriptFlushAction.run();
        }
    }


}
