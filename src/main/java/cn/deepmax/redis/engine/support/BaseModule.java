package cn.deepmax.redis.engine.support;

import cn.deepmax.redis.engine.Module;
import cn.deepmax.redis.engine.RedisCommand;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseModule implements Module {
    protected final String name;
    private final List<RedisCommand> commands = new ArrayList<>();

    public BaseModule(String name) {
        this.name = name;
    }

    @Override
    public List<RedisCommand> commands() {
        return commands;
    }

    @Override
    public String moduleName() {
        return name;
    }

    protected void register(RedisCommand command) {
        commands.add(command);
    }

}
