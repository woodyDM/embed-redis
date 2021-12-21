package cn.deepmax.redis.core;

import cn.deepmax.redis.api.PubsubManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.RegexUtils;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class DefaultPubsub implements PubsubManager {

    private final Pubsub n = new Direct();
    private final Pubsub r = new PatternPubSub();

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
            List<Redis.Client> channels = container.get(channel);
            if (channels == null) {
                return Collections.emptyList();
            }
            List<RedisMessage> msg = new ArrayList<>();
            msg.add(FullBulkValueRedisMessage.ofString("message"));
            msg.add(FullBulkValueRedisMessage.ofString(channel.getContent()));
            msg.add(FullBulkValueRedisMessage.ofString(message));
            return channels.stream().map(ch -> new PubPair(ch, new ListRedisMessage(msg))).collect(Collectors.toList());
        }

    }

    private class PatternPubSub extends BasePubsub {

        private final Map<Key, Pattern> patternMap = new LinkedHashMap<>();

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
            for (Map.Entry<Key, List<Redis.Client>> entry : container.entrySet()) {
                Key p = entry.getKey();
                List<Redis.Client> chs = entry.getValue();
                if (chs != null && !chs.isEmpty()) {
                    Pattern pattern = patternMap.get(p);
                    boolean match = pattern.matcher(channel.str()).find();
                    if (match) {
                        List<RedisMessage> msg = new ArrayList<>();
                        msg.add(FullBulkValueRedisMessage.ofString("pmessage"));
                        msg.add(FullBulkValueRedisMessage.ofString(p.getContent()));
                        msg.add(FullBulkValueRedisMessage.ofString(channel.getContent()));
                        msg.add(FullBulkValueRedisMessage.ofString(message));
                        for (Redis.Client ch : chs) {
                            result.add(new PubPair(ch, new ListRedisMessage(msg)));
                        }
                    }
                }
            }
            return result;
        }

        @Override
        protected void postSub(Redis.Client client, Key channel) {
            patternMap.computeIfAbsent(channel, c -> {
                String regx = RegexUtils.toRegx(channel.str());
                return Pattern.compile(regx);
            });
        }
    }

    abstract class BasePubsub implements Pubsub {
        protected final Map<Key, List<Redis.Client>> container = new LinkedHashMap<>();

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
        public void pub(PubPair pubPair) {
            pubPair.getClient().send(pubPair.getMsg());
        }

        @Override
        public List<RedisMessage> sub(Redis.Client client, List<Key> patternChannel) {
            if (inValidChannels(patternChannel)) {
                return Collections.emptyList();
            }
            List<RedisMessage> result = new ArrayList<>();
            for (Key ch : patternChannel) {
                List<Redis.Client> old = container.computeIfAbsent(ch, k -> new LinkedList<>());
                boolean exist = old.stream().anyMatch(i -> i.equals(client));
                if (!exist) {
                    old.add(client);
                }
                postSub(client, ch);
                ListRedisMessage oneMsg = ListRedisMessage.newBuilder()
                        .append(name())
                        .append(ch.getContent())
                        .append(new IntegerRedisMessage(DefaultPubsub.this.subscribeCount(client))).build();
                result.add(oneMsg);
            }
            return result;
        }

        protected void postSub(Redis.Client client, Key channel) {
        }

        @Override
        public List<RedisMessage> unsubAll(Redis.Client client) {
            List<RedisMessage> msg = new ArrayList<>();
            container.forEach((k, v) -> {
                Iterator<Redis.Client> it = v.iterator();
                while (it.hasNext()) {
                    Redis.Client c = it.next();
                    if (c == client) {
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
        public List<RedisMessage> unsub(Redis.Client client, List<Key> channel) {
            List<RedisMessage> result = new ArrayList<>();
            for (Key key : channel) {
                List<Redis.Client> list = container.get(key);
                if (list != null) {
                    list.remove(client);
                }
                result.add(createUnsubMessage(client, key));
            }
            return result;
        }

        private RedisMessage createUnsubMessage(Redis.Client client, Key key) {
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
        public void quit(Redis.Client client) {
            container.forEach((k, chs) -> chs.remove(client));
        }

        @Override
        public long subCount(Redis.Client client) {
            long c = 0;
            for (Map.Entry<Key, List<Redis.Client>> en : container.entrySet()) {
                if (en.getValue().contains(client)) c++;
            }
            return c;
        }

        private boolean inValidChannels(List<Key> channel) {
            return channel == null || channel.size() == 0;
        }
    }
}

