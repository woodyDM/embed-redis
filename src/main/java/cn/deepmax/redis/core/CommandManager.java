package cn.deepmax.redis.core;

import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
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

    public static final RedisCommand UNKNOWN_COMMAND = ((type, ctx, engine) -> new ErrorRedisMessage("ERR unknown command"));
    private final Map<String, Module> modules = new ConcurrentHashMap<>();
    private final Map<String, RedisCommand> commandMap = new ConcurrentHashMap<>();

    public void load(Module module) {
        List<RedisCommand> commands = module.commands();
        if (commands != null) {
            for (RedisCommand command : commands) {
                String commandName = command.name();
                Module old = modules.put(commandName, module);
                commandMap.put(commandName, command);
                if (old != null) {
                    log.warn("Same command [{}] found at module {} and {}, the command in {} will take effect. ", commandName, module.moduleName(),
                            old.moduleName(), module.moduleName());
                }
            }
            String commandNames = commands.stream().map(RedisCommand::name).collect(Collectors.joining(","));
            log.info("Load module [{}] with {} commands:[{}].", module.moduleName(), commands.size(), commandNames);
        }
    }

    public RedisCommand getCommand(RedisMessage msgo) {
        if (msgo instanceof ListRedisMessage) {
            ListRedisMessage msg = (ListRedisMessage) msgo;
            RedisMessage cmd = msg.children().get(0);
            if (cmd instanceof FullBulkValueRedisMessage) {
                FullBulkValueRedisMessage fm = (FullBulkValueRedisMessage) cmd;
                String strCmd = fm.content().toString(StandardCharsets.UTF_8).toLowerCase();
                RedisCommand redisCommand = commandMap.get(strCmd);
                if (redisCommand != null) {
                    return redisCommand;
                }
            }

        }
        ReferenceCountUtil.release(msgo);
        return UNKNOWN_COMMAND;
    }

}
