package cn.deepmax.redis.engine;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseModule implements Module {
    private final List<RedisCommand> commands = new ArrayList<>();
    protected final String name;


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
