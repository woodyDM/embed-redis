package cn.deepmax.redis.core;

import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.module.*;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.type.RedisType;
import lombok.NonNull;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class DefaultRedisEngine implements RedisEngine {

    private CommandManager commandManager = new CommandManager();
    private PubsubManager pubsubManager = new DefaultPubsub();
    protected TimeProvider timeProvider = new DefaultTimeProvider();
    private DbManager dbManager = new DefaultDbManager(16);
    private RedisConfiguration configuration;
    private final RedisExecutor executor = new DefaultRedisExecutor();
    private final NettyAuthManager authManager = new NettyAuthManager();
    private static final DefaultRedisEngine S = new DefaultRedisEngine();

    public static DefaultRedisEngine instance() {
        return S;
    }

    static {
        S.commandManager.load(new StringModule());
        S.commandManager.load(new HandShakeModule());
        S.commandManager.load(new CommonModule());
        S.commandManager.load(new LuaModule());
        S.commandManager.load(new AuthModule());
        S.commandManager.load(new PubsubModule());
        S.commandManager.load(new DatabaseModule());
    }

    @Override
    public RedisConfiguration configuration() {
        return configuration;
    }

    @Override
    public void setConfiguration(RedisConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public DbManager getDbManager() {
        return dbManager;
    }

    public CommandManager getCommandManager() {
        return commandManager;
    }

    @Override
    public RedisType execute(RedisType type, Redis.Client client) {
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

}