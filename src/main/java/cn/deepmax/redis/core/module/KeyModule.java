package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

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
            engine.getDbManager().fireChangeEvents(client, eventList);
            return new IntegerRedisMessage(eventList.size());
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
