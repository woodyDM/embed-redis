package cn.deepmax.redis.core.support;

import cn.deepmax.redis.core.Module;
import cn.deepmax.redis.core.RedisCommand;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseModule implements Module {
    protected final String name;
    private final Map<String, RedisCommand> commands = new HashMap<>();

    public BaseModule(String name) {
        this.name = name;
    }

    @Override
    public Map<String, RedisCommand> commands() {
        return commands;
    }

    @Override
    public String moduleName() {
        return name;
    }

    protected void register(RedisCommand command) {
        commands.put(command.name(), command);
    }

    protected void register(String name, RedisCommand command) {
        commands.put(name, command);
    }

}
