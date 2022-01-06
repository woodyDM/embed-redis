package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RPattern;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyModule extends BaseModule {

    public KeyModule() {
        super("key");
        register(new Del());
        register(new Exists());
        register(new Expire());
        register(new ExpireAt());
        register(new PExpire());
        register(new PExpireAt());
        register(new Ttl());
        register(new Pttl());
        register(new Persist());
        register(new Scan());
    }

    public static class Scan extends ArgsCommand.Two {
        @SuppressWarnings("unchecked")
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            Long cursor = msg.getAt(1).val();
            Optional<String> pattern = ArgParser.parseArg(msg, 2, "match");
            Long count = ArgParser.parseArg(msg, 2, "count").map(NumberUtils::parse)
                    .orElse(10L);
            Optional<String> type = ArgParser.parseArg(msg, 2, "type");
            Object container = engine.getDb(client).getContainer();
            if (container instanceof ScanMap<?, ?>) {
                return genericScan((ScanMap<Key, ?>) container, true, cursor, count, pattern, type);
            } else {
                return Constants.ERR_IMPL_MISMATCH;
            }
        }
    }

    /**
     * @param map
     * @param globalMap 和type配合，false时，忽略type
     * @param cursor
     * @param count
     * @param pattern
     * @param type      globalMap=true时有效，对key忽略
     * @return
     */
    static RedisMessage genericScan(ScanMap<Key, ?> map, boolean globalMap, Long cursor, Long count, Optional<String> pattern, Optional<String> type) {
        Optional<RPattern> p = pattern.map(RPattern::compile);
        Function<Key, Boolean> mapper = k -> !p.isPresent() || p.get().matches(k.str());
        ScanMap.ScanResult<Key> result = map.scan(cursor, count, mapper);
        List<RedisMessage> keys = result.getKeyNames().stream().map(k -> FullBulkValueRedisMessage.ofString(k.getContent()))
                .collect(Collectors.toList());
        return ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString(result.getNextCursor().toString()))
                .append(new ListRedisMessage(keys))
                .build();
    }

    private static class Del extends ArgsCommand.Two {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<DbManager.KeyEvent> eventList = new ArrayList<>();
            LocalDateTime now = LocalDateTime.now();
            int db = engine.getDbManager().getIndex(client);
            for (int i = 1; i < msg.children().size(); i++) {
                byte[] key = msg.getAt(i).bytes();
                RedisObject old = engine.getDb(client).del(client, key);
                if (old != null) {
                    eventList.add(new DbManager.KeyEvent(key, db, DbManager.EventType.DEL, now));
                }
            }
            int size = eventList.size();
            //should get size before fire events
            engine.getDbManager().fireChangeEvents(client, eventList);
            return new IntegerRedisMessage(size);
        }
    }

    private static class Exists extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            int c = 0;
            for (int i = 1; i < msg.children().size(); i++) {
                RedisObject old = engine.getDb(client).get(client, msg.getAt(i).bytes());
                if (old != null) c++;
            }
            return new IntegerRedisMessage(c);
        }
    }

    private static class Expire extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisObject obj = engine.getDb(client).get(client, msg.getAt(1).bytes());
            if (obj == null) {
                return new IntegerRedisMessage(0);
            }
            obj.expire(msg.getAt(2).val());
            return new IntegerRedisMessage(1);
        }
    }

    private static class PExpire extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisObject obj = engine.getDb(client).get(client, msg.getAt(1).bytes());
            if (obj == null) {
                return new IntegerRedisMessage(0);
            }
            obj.pexpire(msg.getAt(2).val());
            return new IntegerRedisMessage(1);
        }
    }

    private static class ExpireAt extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisObject obj = engine.getDb(client).get(client, msg.getAt(1).bytes());
            if (obj == null) {
                return new IntegerRedisMessage(0);
            }
            ZoneOffset offset = OffsetDateTime.now().getOffset();
            Long timestamp = msg.getAt(2).val();
            LocalDateTime at = LocalDateTime.ofEpochSecond(timestamp, 0, offset);
            obj.expireAt(at);
            return new IntegerRedisMessage(1);
        }
    }

    private static class PExpireAt extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisObject obj = engine.getDb(client).get(client, msg.getAt(1).bytes());
            if (obj == null) {
                return new IntegerRedisMessage(0);
            }
            Long timestamp = msg.getAt(2).val();
            LocalDateTime at = new Timestamp(timestamp).toLocalDateTime();
            obj.expireAt(at);
            return new IntegerRedisMessage(1);
        }
    }

    private static class Ttl extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisObject obj = engine.getDb(client).get(client, msg.getAt(1).bytes());
            if (obj == null) {
                return new IntegerRedisMessage(-2);
            }
            return new IntegerRedisMessage(obj.ttl());
        }
    }

    private static class Pttl extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisObject obj = engine.getDb(client).get(client, msg.getAt(1).bytes());
            if (obj == null) {
                return new IntegerRedisMessage(-2);
            }
            return new IntegerRedisMessage(obj.pttl());
        }
    }

    private static class Persist extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisObject obj = engine.getDb(client).get(client, msg.getAt(1).bytes());
            if (obj == null || obj.expireTime() == null) {
                return new IntegerRedisMessage(0);
            }
            obj.persist();
            return new IntegerRedisMessage(1);
        }
    }

}
