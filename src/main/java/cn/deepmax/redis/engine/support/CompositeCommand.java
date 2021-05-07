package cn.deepmax.redis.engine.support;

import cn.deepmax.redis.engine.CommandManager;
import cn.deepmax.redis.engine.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class CompositeCommand implements RedisCommand {
    private final Map<String, RedisCommand> child = new ConcurrentHashMap<>();
    private String root;

    public CompositeCommand(String root) {
        this.root = root;
    }

    public void add(RedisCommand command) {
        child.put(command.name().toLowerCase(), command);
    }

    @Override
    public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
        String childCommand = type.get(1).str();
        RedisCommand c = child.get(childCommand.toLowerCase());
        if (c == null) {
            c = CommandManager.UNSUPPORTED;
        }
        return c.response(type, ctx, engine);
    }
    
    @Override
    public String name() {
        return root;
    }
}
