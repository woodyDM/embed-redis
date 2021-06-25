package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisArray;
import cn.deepmax.redis.type.RedisBulkString;
import cn.deepmax.redis.utils.RegexUtils;
import io.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class DefaultPubsub implements PubsubManager {

    private Pubsub n = new Normal();
    private Pubsub r = new Regex();

    @Override
    public Pubsub normal() {
        return n;
    }

    @Override
    public Pubsub regex() {
        return r;
    }

    static class Normal extends BasePubsub {
        @Override
        public Set<PubPair> matches(Key channel, byte[] message) {
            Set<Redis.Client> channels = container.get(channel);
            if (channels == null) {
                return Collections.emptySet();
            }
            RedisArray msg = new RedisArray();
            msg.add(RedisBulkString.of("message"));
            msg.add(RedisBulkString.of(channel.getContent()));
            msg.add(RedisBulkString.of(message));
            return channels.stream().map(ch -> new PubPair(ch, msg)).collect(Collectors.toSet());
        }

    }

    static class Regex extends BasePubsub {
        
        private final Map<Key, Pattern> patternMap = new ConcurrentHashMap<>();

        @Override
        public Set<PubPair> matches(Key channel, byte[] message) {
            Set<PubPair> result = new HashSet<>();
            for (Map.Entry<Key, Set<Redis.Client>> entry : container.entrySet()) {
                Key p = entry.getKey();
                Set<Redis.Client> chs = entry.getValue();
                if (chs != null && !chs.isEmpty()) {
                    Pattern pattern = patternMap.get(p);
                    boolean match = pattern.matcher(channel.str()).find();
                    if (match) {
                        RedisArray msg = new RedisArray();
                        msg.add(RedisBulkString.of("pmessage"));
                        msg.add(RedisBulkString.of(p.getContent()));
                        msg.add(RedisBulkString.of(channel.getContent()));
                        msg.add(RedisBulkString.of(message));
                        for (Redis.Client ch : chs) {
                            result.add(new PubPair(ch, msg));
                        }
                    }
                }
            }
            return result;
        }

        @Override
        protected void postSub(Redis.Client client, Key channel) {
            String regx = RegexUtils.toRegx(channel.str());
            patternMap.put(channel, Pattern.compile(regx));
        }
    }

    static abstract class BasePubsub implements Pubsub {
        protected final Map<Key, Set<Redis.Client>> container = new HashMap<>();

        @Override
        public void pub(PubPair pubPair) {
            pubPair.getClient().send(pubPair.getMsg());
        }

        @Override
        public void sub(Redis.Client client, Key... channel) {
            if (inValidChannels(channel)) {
                return;
            }
            for (Key ch : channel) {
                Set<Redis.Client> value = container.computeIfAbsent(ch, (key) -> new HashSet<>());
                value.add(client);
                postSub(client, ch);
            }
        }

        protected void postSub(Redis.Client client, Key channel) {
        }

        @Override
        public void unsub(Redis.Client client, Key... channel) {
            if (inValidChannels(channel)) {
                return;
            }
            for (Key ch : channel) {
                Set<Redis.Client> chs = container.get(ch);
                if (chs != null) {
                    chs.remove(client);
                }
            }
        }

        @Override
        public void quit(Redis.Client client) {
            container.forEach((k, chs) -> chs.remove(client));
        }

        private boolean inValidChannels(Key... channel) {
            return channel == null || channel.length == 0;
        }
    }
}

