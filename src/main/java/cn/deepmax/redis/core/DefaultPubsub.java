package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.PubsubManager;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class DefaultPubsub implements PubsubManager {

    private final Direct n = new Direct();
    private final PatternPubSub r = new PatternPubSub();

    /**
     * subscribe client nubmers
     *
     * @param keys
     * @return
     */
    @Override
    public Map<Key, Integer> numbersub(List<Key> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Key, Integer> map = new LinkedHashMap<>();
        n.container.forEach((k, c) -> map.put(k, c.size()));
        return map;
    }

    /**
     * psubscribe patterns
     *
     * @return
     */
    @Override
    public long numberPattern() {
        return r.container.size();
    }

    /**
     * @param pattern nullable
     * @return
     */
    @Override
    public List<Key> channelNumbers(RPattern pattern) {
        Set<Key> channels = n.container.keySet();
        if (pattern == null) {
            return new ArrayList<>(channels);
        }
        return channels.stream().filter(k -> pattern.matches(k.str())).collect(Collectors.toList());
    }

    @Override
    public Pubsub direct() {
        return n;
    }

    @Override
    public Pubsub pattern() {
        return r;
    }

    private class Direct extends BasePubsub {
        @Override
        protected String name() {
            return "subscribe";
        }

        @Override
        protected String unname() {
            return "unsubscribe";
        }

        @Override
        public List<PubPair> matches(Key channel, byte[] message) {
            List<Client> channels = container.get(channel);
            if (channels == null) {
                return Collections.emptyList();
            }
            return channels.stream().map(ch -> {
                List<RedisMessage> msg = new ArrayList<>();
                msg.add(FullBulkValueRedisMessage.ofString("message"));
                msg.add(FullBulkValueRedisMessage.ofString(channel.getContent()));
                msg.add(FullBulkValueRedisMessage.ofString(message));
                return new PubPair(ch, new ListRedisMessage(msg));
            }).collect(Collectors.toList());
        }

    }

    private class PatternPubSub extends BasePubsub {

        @Override
        protected String name() {
            return "psubscribe";
        }

        @Override
        protected String unname() {
            return "punsubscribe";
        }

        @Override
        public List<PubPair> matches(Key channel, byte[] message) {
            List<PubPair> result = new ArrayList<>();
            //key:pattern  value:client sub to pattern
            for (Map.Entry<Key, List<Client>> entry : container.entrySet()) {
                Key pt = entry.getKey();
                List<Client> chs = entry.getValue();
                if (chs != null && !chs.isEmpty()) {
                    boolean match = RPattern.compile(pt.str()).matches(channel.str());
                    if (match) {
                        for (Client ch : chs) {
                            List<RedisMessage> msg = new ArrayList<>();
                            msg.add(FullBulkValueRedisMessage.ofString("pmessage"));
                            msg.add(FullBulkValueRedisMessage.ofString(pt.getContent()));
                            msg.add(FullBulkValueRedisMessage.ofString(channel.getContent()));
                            msg.add(FullBulkValueRedisMessage.ofString(message));
                            result.add(new PubPair(ch, new ListRedisMessage(msg)));
                        }
                    }
                }
            }
            return result;
        }
    }

    abstract class BasePubsub implements Pubsub {
        protected final Map<Key, List<Client>> container = new LinkedHashMap<>();

        /**
         * message name : subscribe / psubscribe
         *
         * @return
         */
        abstract protected String name();

        /**
         * message name : unsubscribe / punsubscribe
         * @return
         */
        abstract protected String unname();

        @Override
        public List<RedisMessage> sub(Client client, List<Key> patternChannel) {
            if (inValidChannels(patternChannel)) {
                return Collections.emptyList();
            }
            List<RedisMessage> result = new ArrayList<>();
            for (Key ch : patternChannel) {
                List<Client> old = container.computeIfAbsent(ch, k -> new LinkedList<>());
                boolean exist = old.stream().anyMatch(i -> i.equals(client));
                if (!exist) {
                    old.add(client);
                }
                ListRedisMessage oneMsg = ListRedisMessage.newBuilder()
                        .append(name())
                        .append(ch.getContent())
                        .append(new IntegerRedisMessage(DefaultPubsub.this.subscribeCount(client))).build();
                result.add(oneMsg);
            }
            return result;
        }

        @Override
        public List<RedisMessage> unsubAll(Client client) {
            List<RedisMessage> msg = new ArrayList<>();
            container.forEach((k, v) -> {
                Iterator<Client> it = v.iterator();
                while (it.hasNext()) {
                    Client c = it.next();
                    if (c.equals(client)) {
                        it.remove();
                        msg.add(createUnsubMessage(c, k));
                        break;
                    }
                }
            });
            if (msg.isEmpty()) {
                return Collections.singletonList(createUnsubMessage(client, null));
            } else {
                return msg;
            }
        }

        @Override
        public List<RedisMessage> unsub(Client client, List<Key> channel) {
            List<RedisMessage> result = new ArrayList<>();
            for (Key key : channel) {
                List<Client> list = container.get(key);
                if (list != null) {
                    list.remove(client);
                }
                result.add(createUnsubMessage(client, key));
            }
            return result;
        }

        private RedisMessage createUnsubMessage(Client client, Key key) {
            RedisMessage m = key == null ? FullBulkStringRedisMessage.NULL_INSTANCE :
                    FullBulkValueRedisMessage.ofString(key.getContent());
            long count = DefaultPubsub.this.subscribeCount(client);
            return ListRedisMessage.newBuilder()
                    .append(unname())
                    .append(m)
                    .append(new IntegerRedisMessage(count))
                    .build();
        }

        @Override
        public void quit(Client client) {
            container.forEach((k, chs) -> chs.remove(client));
        }

        @Override
        public long subCount(Client client) {
            long c = 0;
            for (Map.Entry<Key, List<Client>> en : container.entrySet()) {
                if (en.getValue().contains(client)) c++;
            }
            return c;
        }

        private boolean inValidChannels(List<Key> channel) {
            return channel == null || channel.size() == 0;
        }
    }
}

