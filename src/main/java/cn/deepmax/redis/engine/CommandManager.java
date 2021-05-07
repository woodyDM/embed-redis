package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisType;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/4/30
 */
@Slf4j
public class CommandManager {

    private final Map<String, Module> modules = new ConcurrentHashMap<>();
    private final Map<String, RedisCommand> commandMap = new ConcurrentHashMap<>();
    public static final RedisCommand UNSUPPORTED = ((type, ctx, engine) -> new RedisError("unsupported command"));

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
            String commandNames = commands.stream().map(RedisCommand::name).collect(Collectors.joining(","));
            log.info("Load module [{}] with {} commands:[{}].", module.moduleName(), commands.size(), commandNames);
        }
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

}
