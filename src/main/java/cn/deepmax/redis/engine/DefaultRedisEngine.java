package cn.deepmax.redis.engine;

import cn.deepmax.redis.engine.module.*;
import cn.deepmax.redis.infra.DefaultTimeProvider;
import cn.deepmax.redis.infra.TimeProvider;
import cn.deepmax.redis.type.RedisType;
import lombok.NonNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class DefaultRedisEngine implements RedisEngine {

    private CommandManager commandManager = new CommandManager();
    
    protected TimeProvider timeProvider = new DefaultTimeProvider();
    private final Map<Key, RedisObject> data = new ConcurrentHashMap<>();
    private final RedisExecutor executor = new RedisExecutor();
    private final DefaultAuthManager authManager = new DefaultAuthManager();
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
    }
    
    public CommandManager getCommandManager() {
        return commandManager;
    }
    
    @Override
    public RedisCommand getCommand(RedisType type) {
        return commandManager.getCommand(type);
    }

    @Override
    public RedisExecutor executor() {
        return executor;
    }

    public void setTimeProvider(TimeProvider timeProvider) {
        this.timeProvider = Objects.requireNonNull(timeProvider);
    }

    @Override
    public RedisObject set(byte[] key, RedisObject newValue) {
        return data.put(new Key(key), newValue);
    }

    @Override
    public RedisObject get(byte[] key) {
        return data.get(new Key(key));
    }

    @Override
    public RedisObject del(byte[] key) {
        return data.remove(new Key(key));
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

    static class Key {
        byte[] content;

        Key(byte[] content) {
            this.content = content;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Arrays.equals(content, key.content);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(content);
        }
    }

}
