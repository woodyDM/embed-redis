package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.SizedOperation;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class SetModule extends BaseModule {
    public SetModule() {
        super("set");
        register(new SAdd());
        register(new SCard());
        register(new SIsMember());
        register(new SMIsMember());
        register(new SPop(false, "srandmember"));
        register(new SPop(true, "spop"));
        register(new SRem());
        register(new SMembers());
        register(new SMove());
        register(new SDiff());
        register(new SDiffStore());
        register(new SInter());
        register(new SInterStore());
        register(new SUnion());
        register(new SUnionStore());
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

    public static class SRem extends ArgsCommand.ThreeWith<RSet> implements SizedOperation {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> members = genKeys(msg.children(), 2);
            RSet set = get(key);
            if (set == null) {
                return Constants.INT_ZERO;
            }
            int eff = set.remove(members);
            deleteEleIfNeed(key, engine, client);
            return new IntegerRedisMessage(eff);
        }
    }

    public static class SMembers extends ArgsCommand.TwoExWith<RSet> implements SizedOperation {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RSet set = get(key);
            if (set == null) {
                return ListRedisMessage.empty();
            }
            List<Key> list = set.members();
            return ListRedisMessage.wrapKeys(list);
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

    public static class SPop extends ArgsCommand<RSet> implements SizedOperation {
        final boolean remove;
        final String name;

        public SPop(boolean remove, String name) {
            super(2, 3);
            this.remove = remove;
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Optional<ArgParser.CountArg> argO = ArgParser.parseCount(msg, 2);
            if (!argO.isPresent()) {
                return Constants.ERR_SYNTAX;
            }
            ArgParser.CountArg arg = argO.get();
            if (remove && arg.count < 0) {
                return Constants.ERR_SYNTAX;
            }
            RSet set = get(key);
            if (set == null) {
                return arg.withCount ? ListRedisMessage.empty() : FullBulkValueRedisMessage.NULL_INSTANCE;
            }
            List<Key> members = set.randomMember(arg.count);
            if (remove) {
                set.remove(members);
                deleteEleIfNeed(key, engine, client);
            }
            if (arg.withCount) {
                return ListRedisMessage.wrapKeys(members);
            } else {
                return FullBulkValueRedisMessage.ofString(members.get(0).getContent());
            }
        }
    }

    public static class SMove extends ArgsCommand.FourExWith<RSet> implements SizedOperation {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] source = msg.getAt(1).bytes();
            byte[] dest = msg.getAt(2).bytes();
            Key member = msg.getAt(3).key();
            RSet s = get(source);
            if (s == null) {
                return Constants.INT_ZERO;
            }
            Boolean v = s.get(member);
            if (v == null) {
                return Constants.INT_ZERO;
            }
            s.remove(member);
            deleteEleIfNeed(source, engine, client);
            RSet d = get(dest);
            if (d == null) {
                d = new RSet(engine.timeProvider());
                d.set(member, true);
                engine.getDb(client).set(client, dest, d);
                return Constants.INT_ONE;
            } else {
                Boolean dv = d.get(member);
                if (dv != null) {
                    return Constants.INT_ONE;
                }
                d.set(member, true);
                engine.fireChangeEvent(client, dest, DbManager.EventType.UPDATE);
                return Constants.INT_ONE;
            }
        }
    }

    public static class SDiff extends ArgsCommand<RSet> {
        public SDiff() {
            super(2);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            List<Key> keys = genKeys(msg.children(), 2);
            List<RSet> sets = keys.stream().map(k -> get(k.getContent())).filter(Objects::nonNull).collect(Collectors.toList());
            RSet set = get(key);
            if (set == null) {
                return ListRedisMessage.empty();
            }
            List<Key> diff = set.diff(sets);
            return ListRedisMessage.wrapKeys(diff);
        }
    }

    //SINTER key [key ...]
    public static class SInter extends ArgsCommand<RSet> {
        public SInter() {
            super(2);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<Key> keys = genKeys(msg.children(), 1);
            List<RSet> sets = keys.stream().map(k -> get(k.getContent())).collect(Collectors.toList());
            boolean hasEmpty = sets.stream().anyMatch(Objects::isNull);
            if (hasEmpty) {
                return ListRedisMessage.empty();
            }
            sets = sets.stream().filter(Objects::nonNull).collect(Collectors.toList());
            List<Key> inter = RSet.inter(sets);
            return ListRedisMessage.wrapKeys(inter);
        }
    }

    //SUNION key [key ...] 
    public static class SUnion extends ArgsCommand<RSet> {
        public SUnion() {
            super(2);
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<Key> keys = genKeys(msg.children(), 1);
            List<RSet> sets = keys.stream().map(k -> get(k.getContent())).filter(Objects::nonNull).collect(Collectors.toList());
            if (sets.isEmpty()) {
                return ListRedisMessage.empty();
            }
            List<Key> inter = RSet.union(sets);
            return ListRedisMessage.wrapKeys(inter);
        }
    }

    public static class SDiffStore extends ArgsCommand.ThreeWith<RSet> {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] dest = msg.getAt(1).bytes();
            byte[] key = msg.getAt(2).bytes();
            List<Key> keys = genKeys(msg.children(), 3);
            List<RSet> sets = keys.stream().map(k -> get(k.getContent())).filter(Objects::nonNull).collect(Collectors.toList());
            //del old dest
            engine.getDb(client).del(client, dest);
            RSet set = get(key);
            if (set == null) {
                return Constants.INT_ZERO;
            }
            List<Key> diff = set.diff(sets);
            if (diff.isEmpty()) {
                return Constants.INT_ZERO;
            }
            RSet destSet = new RSet(engine.timeProvider());
            destSet.add(diff);
            engine.getDb(client).set(client, dest, destSet);
            return new IntegerRedisMessage(diff.size());
        }
    }

    //SINTERSTORE destination key [key ...]
    public static class SInterStore extends ArgsCommand.ThreeWith<RSet> {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] dest = msg.getAt(1).bytes();
            List<Key> keys = genKeys(msg.children(), 2);
            List<RSet> sets = keys.stream().map(k -> get(k.getContent())).collect(Collectors.toList());
            boolean hasEmpty = sets.stream().anyMatch(Objects::isNull);
            engine.getDb(client).del(client, dest);
            if (hasEmpty) {
                return Constants.INT_ZERO;
            }
            sets = sets.stream().filter(Objects::nonNull).collect(Collectors.toList());
            List<Key> inter = RSet.inter(sets);
            if (inter.isEmpty()) {
                return Constants.INT_ZERO;
            }
            RSet destSet = new RSet(engine.timeProvider());
            destSet.add(inter);
            engine.getDb(client).set(client, dest, destSet);
            return new IntegerRedisMessage(inter.size());
        }
    }

    //SUNIONSTORE destination key [key ...]
    public static class SUnionStore extends ArgsCommand.ThreeWith<RSet> {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] dest = msg.getAt(1).bytes();
            List<Key> keys = genKeys(msg.children(), 2);
            List<RSet> sets = keys.stream().map(k -> get(k.getContent())).filter(Objects::nonNull).collect(Collectors.toList());
            engine.getDb(client).del(client, dest);
            List<Key> union = RSet.union(sets);
            if (union.isEmpty()) {
                return Constants.INT_ZERO;
            }
            RSet destSet = new RSet(engine.timeProvider());
            destSet.add(union);
            engine.getDb(client).set(client, dest, destSet);
            return new IntegerRedisMessage(union.size());
        }
    }

}
