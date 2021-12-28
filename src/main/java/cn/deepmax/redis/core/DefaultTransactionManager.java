package cn.deepmax.redis.core;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author wudi
 */
public class DefaultTransactionManager implements TransactionManager {
    private final RedisEngine engine;
    private final Map<Client, Boolean> watchKeys = new HashMap<>();
    private final Map<Client, List<DbManager.KeyEventListener>> listHashMap = new HashMap<>();
    private final Map<Client, List<RedisMessage>> commands = new HashMap<>();
    private final Set<Client> errorClient = new HashSet<>();
    private static final Set<String> txWhiteList = new HashSet<>();

    static {
        txWhiteList.add("discard");
        txWhiteList.add("multi");
        txWhiteList.add("watch");
        txWhiteList.add("unwatch");
    }

    public DefaultTransactionManager(RedisEngine engine) {
        this.engine = engine;
    }

    @Override
    public void multi(Client client) {
        client.setQueue(true);
    }

    @Override
    public RedisMessage exec(Client client) {
        try {
            //set queue to false to allow engine to exec command. Otherwise, command will be queued!
            client.setQueue(false);
            if (errorClient.contains(client)) {
                return new ErrorRedisMessage("EXECABORT Transaction discarded because of previous errors.");
            }
            if (!inspect(client)) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            //set queue_exec to true to allow queued events.
            client.setFlag(Client.FLAG_QUEUE_EXEC, true);
            List<RedisMessage> cmds = commands.getOrDefault(client, Collections.emptyList());
            List<RedisMessage> resps = cmds.stream().map(c -> engine.execute(c, client))
                    .collect(Collectors.toList());
            client.setFlag(Client.FLAG_QUEUE_EXEC, false);
            //fire all queued key events;
            engine.getDbManager().fireChangeQueuedEvents(client);
            return new ListRedisMessage(resps);
        } finally {
            unwatch(client);
        }
    }
    
    /**
     * Intercept all command (except exec)
     * test exec if any syntax error. or return QUEUED
     */
    @Override
    public RedisMessage queue(Client client, RedisMessage msg) {
        RedisCommand cmd = engine.commandManager().getCommand(msg);
        if (cmd == Constants.UNKNOWN_COMMAND) {
            errorClient.add(client);
            return cmd.response(msg, client, engine);
        }
        if (cmd instanceof ArgsCommand<?>) {
            Optional<ErrorRedisMessage> error = ((ArgsCommand<?>) cmd).preCheckLength(msg);
            if (error.isPresent()) {
                errorClient.add(client);
                return error.get();
            }
        }
        if (txWhiteList.contains(cmd.name())) {
            return cmd.response(msg, client, engine);
        }
        //Only add messages when no error
        if (!errorClient.contains(client)) {
            ReferenceCountUtil.retain(msg);
            List<RedisMessage> list = commands.computeIfAbsent(client, k -> new LinkedList<>());
            list.add(msg);
        }
        return Constants.QUEUED;
    }

    @Override
    public void watch(Client client, List<Key> keys) {
        watchKeys.computeIfAbsent(client, k -> true);
        listHashMap.computeIfAbsent(client, k -> new ArrayList<>());
        DbManager.KeyEventListener theListener = (k, listener) -> {
            watchKeys.put(client, false);
            removeListeners(client);
        };
        listHashMap.get(client).add(theListener);
        engine.getDbManager().addListener(client, keys, theListener);
    }

    @Override
    public void unwatch(Client client) {
        watchKeys.remove(client);
        errorClient.remove(client);
        removeListeners(client);
        List<RedisMessage> remove = commands.remove(client);
        if (remove != null) remove.forEach(ReferenceCountUtil::release);
    }

    private void removeListeners(Client client) {
        listHashMap.getOrDefault(client, Collections.emptyList())
                .forEach(inner -> engine.getDbManager().removeListener(inner));
        listHashMap.remove(client);
    }

    /**
     * check watch
     *
     * @param client
     * @return
     */
    @Override
    public boolean inspect(Client client) {
        Boolean valid = this.watchKeys.get(client);
        return valid == null || valid;
    }

    @Override
    public RedisEngine engine() {
        return engine;
    }

}
