package cn.deepmax.redis.core.support;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wudi
 * @date 2022/1/10
 */
public class CompositeCommand implements RedisCommand {

    private final Map<String, RedisCommand> subCommands = new HashMap<>();
    private final String name;

    public CompositeCommand(String name) {
        this.name = name.toLowerCase();
    }

    /**
     * command for search
     *
     * @return
     */
    @Override
    public String name() {
        return name;
    }

    public CompositeCommand with(String subName, RedisCommand command) {
        if (subName.startsWith(this.name)) {
            subCommands.put(subName.substring(this.name.length()).toLowerCase(), command);
            return this;
        } else {
            throw new IllegalStateException("can't put sub command " + subName + "to " + this.name);
        }
    }

    public CompositeCommand with(RedisCommand command) {
        return this.with(command.name(), command);
    }

    @Override
    public RedisMessage response(RedisMessage type, Client client, RedisEngine engine) {
        ListRedisMessage msg = cast(type);
        if (msg.children().size() < 2) {
            return Constants.ERR_SYNTAX;
        }
        String sub = msg.getAt(1).str();
        RedisCommand command = subCommands.get(sub.toLowerCase());
        if (command == null) {
            String e = "ERR Unknown subcommand or wrong number of arguments for '%s'.";
            return new ErrorRedisMessage(String.format(e, sub));
        } else {
            return command.response(type, client, engine);
        }
    }
}
