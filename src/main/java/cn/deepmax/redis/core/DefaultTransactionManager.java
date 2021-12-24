package cn.deepmax.redis.core;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TransactionManager;
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
    private final Map<Redis.Client, Set<WatchKey>> watchKeys = new HashMap<>();
    private final Map<Redis.Client, List<RedisMessage>> commands = new HashMap<>();
    private final Set<Redis.Client> errorClient = new HashSet<>();
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
    public void multi(Redis.Client client) {
        client.setQueue(true);
    }

    @Override
    public RedisMessage exec(Redis.Client client) {
        try {
            if (errorClient.contains(client)) {
                return new ErrorRedisMessage("EXECABORT Transaction discarded because of previous errors.");
            }
            if (!inspect(client)) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            //set queue to false to allow engine.execute
            client.setQueue(false);
            List<RedisMessage> cmds = commands.getOrDefault(client, Collections.emptyList());
            List<RedisMessage> resps = cmds.stream().map(c -> engine.execute(c, client))
                    .collect(Collectors.toList());
            return new ListRedisMessage(resps);
        } finally {
            client.setQueue(false);
            unwatch(client);
        }
    }
    
    /**
     * Intercept all command (except exec)
     * test exec if any syntax error. or return QUEUED
     */
    @Override
    public RedisMessage queue(Redis.Client client, RedisMessage msg) {
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
    public void watch(Redis.Client client, List<Key> keys) {
        Set<WatchKey> set = watchKeys.computeIfAbsent(client, k -> new HashSet<>());
        int dbIdx = engine.getDbManager().getIndex(client);
        RedisEngine.Db db = engine.getDb(client);
        for (Key key : keys) {
            RedisObject obj = db.get(key.getContent());
            OptionalLong l = obj == null ? OptionalLong.empty() : OptionalLong.of(obj.version());
            set.add(new WatchKey(key, dbIdx, l));
        }
    }

    @Override
    public void unwatch(Redis.Client client) {
        watchKeys.remove(client);
        errorClient.remove(client);
        List<RedisMessage> remove = commands.remove(client);
        if (remove != null) remove.forEach(ReferenceCountUtil::release);
    }

    /**
     * check watch
     *
     * @param client
     * @return
     */
    @Override
    public boolean inspect(Redis.Client client) {
        Set<WatchKey> keys = this.watchKeys.get(client);
        if (keys == null || keys.isEmpty()) {
            return true;
        }
        for (WatchKey key : keys) {
            RedisEngine.Db db = engine.getDbManager().get(key.db);
            RedisObject obj = db.get(key.key.getContent());
            if (obj == null && !key.version.isPresent()) {
                continue;
            }
            if (obj != null && key.version.isPresent() && obj.version() == key.version.getAsLong()) {
                continue;
            }
            return false;
        }
        return true;
    }

    @Override
    public RedisEngine engine() {
        return engine;
    }

    /**
     * version not include to identify a unique watch.
     */
    static class WatchKey {
        final Key key;
        final int db;
        final OptionalLong version;

        public WatchKey(Key key, int db, OptionalLong version) {
            this.key = key;
            this.db = db;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WatchKey watchKey = (WatchKey) o;
            return db == watchKey.db &&
                    Objects.equals(key, watchKey.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, db);
        }
    }
}
