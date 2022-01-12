package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.Network;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RPattern;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.CompositeCommand;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KeyModule extends BaseModule {

    public KeyModule() {
        super("key");
        register(new Del());
        register("unlink", new Del());
        register(new Copy());
        register(new Keys());
        register(new Move());
        register(new RandomKey());
        register(new Exists());
        register(new Expire());
        register(new ExpireAt());
        register(new PExpire());
        register(new PExpireAt());
        register(new Ttl());
        register(new Pttl());
        register(new Persist());
        register(new Type());
        register(new Scan());
        register(new Touch());
        register(new Rename("rename", false));
        register(new Rename("renamenx", true));
        register(new CompositeCommand("object")
                .with(new ObjectEncoding())
                .with("objectrefcount", new ObjectCmd())
                .with("objectfreq", new ObjectCmd())
                .with("objectidletime", new ObjectCmd())
        );
    }

    public static class Rename extends ArgsCommand.ThreeEx {
        private final String name;
        private final boolean nx;

        public Rename(String name, boolean nx) {
            this.name = name;
            this.nx = nx;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] newKey = msg.getAt(2).bytes();
            RedisObject s = engine.getDb(client).get(client, key);
            RedisObject dest = engine.getDb(client).get(client, newKey);
            if (s == null) {
                return new ErrorRedisMessage("ERR no such key");
            }
            if (Arrays.equals(key, newKey)) {
                return nx ? Constants.INT_ZERO : OK;
            }
            if (dest != null) {
                if (nx) {
                    return Constants.INT_ZERO;
                } else {
                    engine.getDb(client).del(client, newKey);
                }
            }
            //keep ttl
            engine.getDb(client).del(client, key);
            RedisObject newValue = s.copyTo(new Key(newKey));
            newValue.expireAt(s.expireTime());
            engine.getDb(client).set(client, newKey, newValue);
            return nx ? Constants.INT_ONE : OK;
        }
    }

    public static class Touch extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<Key> keys = genKeys(msg.children(), 1);
            int c = 0;
            for (Key key : keys) {
                if (engine.getDb(client).get(client, key.getContent()) != null) {
                    c++;
                }
            }
            return new IntegerRedisMessage(c);
        }
    }

    public static class ObjectCmd extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(2).bytes();
            RedisObject obj = engine.getDb(client).get(client, key);
            if (obj == null) {
                return Constants.INT_ZERO;
            }
            return Constants.INT_ONE;
        }
    }

    public static class ObjectEncoding extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(2).bytes();
            RedisObject obj = engine.getDb(client).get(client, key);
            if (obj == null) {
                return Network.nullValue(client);
            }
            return FullBulkValueRedisMessage.ofString(obj.type().encoding());
        }
    }

    public static class Type extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RedisObject obj = engine.getDb(client).get(client, key);
            if (obj == null) {
                return new SimpleStringRedisMessage("none");
            }
            return new SimpleStringRedisMessage(obj.type().name());
        }
    }

    public static class Move extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long db = msg.getAt(2).val();
            RedisObject source = engine.getDb(client).get(client, key);
            if (source == null) {
                return Constants.INT_ZERO;
            }
            RedisObject dest = engine.getDbManager().get((int) db).get(client, key);
            if (dest != null) {
                return Constants.INT_ZERO;
            }
            engine.getDb(client).del(client, key);
            engine.getDbManager().get((int) db).set(client, key, source);
            return Constants.INT_ONE;
        }
    }

    public static class Copy extends ArgsCommand.Three {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] source = msg.getAt(1).bytes();
            byte[] dest = msg.getAt(2).bytes();
            Optional<Long> dbo = ArgParser.parseLongArg(msg, "db");
            int db = dbo.map(Long::intValue).orElseGet(() -> engine.getDbManager().getIndex(client));
            boolean replace = ArgParser.parseFlag(msg, "replace", 3);
            RedisObject sourceObj = engine.getDbManager().get(client).get(client, source);
            if (sourceObj == null) {
                return Constants.INT_ZERO;
            }
            RedisObject destObj = engine.getDbManager().get(db).get(client, dest);
            if (replace) {
                engine.getDbManager().get(db).del(client, dest);
            } else if (destObj != null) {
                return Constants.INT_ZERO;
            }
            //to set dest
            RedisObject copy = sourceObj.copyTo(new Key(dest));
            engine.getDbManager().get(db).set(client, dest, copy);
            return Constants.INT_ONE;
        }
    }

    public static class RandomKey extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisEngine.Db db = engine.getDb(client);
            if (db.size() == 0) {
                return Network.nullValue(client);
            }
            Key rd = db.randomKey();
            return FullBulkValueRedisMessage.ofString(rd.getContent());
        }
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

    public static class Keys extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String pt = msg.getAt(1).str();
            RPattern pattern = RPattern.compile(pt);
            Set<Key> keys = engine.getDb(client).keys();
            List<RedisMessage> list = keys.stream().filter(k -> pattern.matches(k.str()))
                    .map(k -> FullBulkValueRedisMessage.ofString(k.getContent()))
                    .collect(Collectors.toList());
            return new ListRedisMessage(list);
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
