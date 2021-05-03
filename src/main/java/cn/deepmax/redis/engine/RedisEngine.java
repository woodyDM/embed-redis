package cn.deepmax.redis.engine;

import cn.deepmax.redis.infra.TimeProvider;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisType;
import lombok.NonNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class RedisEngine {


    private final Map<String, Module> modules = new ConcurrentHashMap<>();
    private final Map<String, RedisCommand> commandMap = new ConcurrentHashMap<>();
    private static final RedisCommand UNSUPPORTED = ((type, ctx) -> new RedisError("unsupported command"));

    public void load(Module module) {
        List<RedisCommand> commands = module.commands();
        if (commands != null) {
            for (RedisCommand command : commands) {
                String commandName = command.name();
                Module old = modules.put(commandName, module);
                commandMap.put(commandName, command);
                if (old != null) {
                    throw new IllegalArgumentException("can't load command " + commandName + " at module " + module.moduleName()
                            + " because command already exist at module " + old.moduleName());
                }
            }
        }
    }

    public void setTimeProvider(@NonNull TimeProvider timeProvider) {
        this.modules.values().forEach(m -> m.setTimeProvider(timeProvider));
    }

    public RedisCommand getCommand(RedisType msg) {
        if (msg.isArray()) {
            RedisType cmd = msg.get(0);
            if (cmd.isString()) {
                String strCmd = cmd.str().toLowerCase();
                RedisCommand redisCommand = commandMap.get(strCmd);
                if (redisCommand != null) {
                    return redisCommand;
                }
            }
        }

        return UNSUPPORTED;
    }

    public static RedisEngine getInstance() {
        return S;
    }

    private static final RedisEngine S = new RedisEngine();

    static {
        S.load(new StringModule());
        S.load(new HandShakeModule());
    }


}
