package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.List;

public class SetModule extends BaseModule {
    public SetModule() {
        super("set");
        register(new SAdd());
        register(new SCard());
        register(new SIsMember());
        register(new SMIsMember());
        register(new SRandMember());
    }

    public static class SAdd extends ArgsCommand.ThreeWith<RSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> members = genKeys(msg.children(), 2);
            RSet set = get(key);
            if (set == null) {
                set = new RSet(engine.timeProvider());
                int i = set.add(members);
                engine.getDb(client).set(client, key, set);
                return new IntegerRedisMessage(i);
            }
            int i = set.add(members);
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            return new IntegerRedisMessage(i);
        }
    }

    public static class SCard extends ArgsCommand.TwoExWith<RSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RSet set = get(key);
            if (set == null) {
                return Constants.INT_ZERO;
            }
            long i = set.size();
            return new IntegerRedisMessage(i);
        }
    }

    public static class SIsMember extends ArgsCommand.ThreeExWith<RSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RSet set = get(key);
            if (set == null) {
                return Constants.INT_ZERO;
            }
            byte[] member = msg.getAt(2).bytes();
            Boolean v = set.get(new Key(member));
            if (v == null) {
                return Constants.INT_ZERO;
            }
            return Constants.INT_ONE;
        }
    }

    public static class SMIsMember extends ArgsCommand.ThreeWith<RSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> members = genKeys(msg.children(), 2);
            RSet set = get(key);
            List<RedisMessage> list = new ArrayList<>();
            for (Key member : members) {
                if (set != null && set.get(member) != null) {
                    list.add(Constants.INT_ONE);
                } else {
                    list.add(Constants.INT_ZERO);
                }
            }
            return new ListRedisMessage(list);

        }
    }

    public static class SRandMember extends ArgsCommand<RSet> {
        public SRandMember() {
            super(2, 3);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long count = 1;
            boolean withCount = false;
            if (msg.children().size() == 3) {
                count = msg.getAt(2).val();
                withCount = true;
            }
            RSet set = get(key);
            if (set == null) {
                return withCount ? ListRedisMessage.empty() : FullBulkValueRedisMessage.NULL_INSTANCE;
            }
            if (count == 0) {
                return Constants.ERR_SYNTAX;
            }
            List<Key> members = set.randomMember(count);
            if (withCount) {
                return ListRedisMessage.wrapKeys(members);
            } else {
                return FullBulkValueRedisMessage.ofString(members.get(0).getContent());
            }
        }
    }
}
