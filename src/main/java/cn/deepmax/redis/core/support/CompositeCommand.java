package cn.deepmax.redis.core.support;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.CommandManager;
import cn.deepmax.redis.core.RedisCommand;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class CompositeCommand implements RedisCommand {
    private final Map<String, RedisCommand> child = new ConcurrentHashMap<>();
    private final String root;

    public CompositeCommand(String root) {
        this.root = root;
    }

    public void add(RedisCommand command) {
        child.put(command.name().toLowerCase(), command);
    }

    @Override
    public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
        String childCommand = cast(type).getAt(1).str();
        RedisCommand c = child.get(childCommand.toLowerCase());
        if (c == null) {
            c = CommandManager.UNKNOWN_COMMAND;
        }
        return c.response(type, client, engine);
    }

    @Override
    public String name() {
        return root;
    }
}
