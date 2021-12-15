package cn.deepmax.redis.core;

import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.module.*;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.NonNull;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class DefaultRedisEngine implements RedisEngine {

    private static final DefaultRedisEngine S = new DefaultRedisEngine();

    static {
        S.commandManager.load(new StringModule());
        S.commandManager.load(new HandShakeModule());
        S.commandManager.load(new CommonModule());
        S.commandManager.load(new LuaModule());
        S.commandManager.load(new AuthModule());
        S.commandManager.load(new PubsubModule());
        S.commandManager.load(new DatabaseModule());
    }

    private final RedisExecutor executor = new DefaultRedisExecutor();
    private final NettyAuthManager authManager = new NettyAuthManager();
    private final CommandManager commandManager = new CommandManager();
    private final PubsubManager pubsubManager = new DefaultPubsub();
    private final DbManager dbManager = new DefaultDbManager(16);
    protected TimeProvider timeProvider = new DefaultTimeProvider();
    private RedisConfiguration configuration;

    public static DefaultRedisEngine instance() {
        return S;
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

}
