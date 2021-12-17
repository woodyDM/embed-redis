package cn.deepmax.redis.core;

import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.module.*;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * @author wudi
 * @date 2021/4/30
 */
@Slf4j
public class DefaultRedisEngine implements RedisEngine {

    private final RedisExecutor executor = new DefaultRedisExecutor();
    private final NettyAuthManager authManager = new NettyAuthManager();
    private final CommandManager commandManager = new CommandManager();
    private final PubsubManager pubsubManager = new DefaultPubsub();
    private final DbManager dbManager = new DefaultDbManager(16);
    protected TimeProvider timeProvider = new DefaultTimeProvider();
    private Runnable scriptFlushAction;
    private RedisConfiguration configuration;

    public DefaultRedisEngine() {
        loadDefaultModules();
    }

    public static DefaultRedisEngine defaultEngine() {
        DefaultRedisEngine e = new DefaultRedisEngine();
        e.loadDefaultModules();
        return e;
    }

    private void loadDefaultModules() {
        loadModule(new StringModule());
        loadModule(new HandShakeModule());
        loadModule(new CommonModule());
        LuaModule luaModule = new LuaModule();
        loadModule(luaModule);
        scriptFlushAction = luaModule::flush;
        loadModule(new AuthModule());
        loadModule(new PubsubModule());
        loadModule(new DatabaseModule());
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

    public CommandManager getCommandManager() {
        return commandManager;
    }

    @Override
    public RedisMessage execute(RedisMessage type, Redis.Client client) {
        return executor.execute(type, this, client);
    }

    public void setTimeProvider(TimeProvider timeProvider) {
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    @Override
    public boolean isExpire(@NonNull RedisObject v) {
        LocalDateTime time = v.expireTime();
        return time != null && timeProvider.now().isAfter(time);
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
    public void scriptFlush() {
        if (scriptFlushAction != null) {
            log.debug("Flush all script");
            scriptFlushAction.run();
        }
    }


}
