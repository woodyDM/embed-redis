package cn.deepmax.redis.core;

import cn.deepmax.redis.api.PubsubManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.RegexUtils;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class DefaultPubsub implements PubsubManager {

    private final Pubsub n = new Normal();
    private final Pubsub r = new Regex();

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
            List<RedisMessage> msg = new ArrayList<>();
            msg.add(FullBulkValueRedisMessage.ofString("message"));
            msg.add(FullBulkValueRedisMessage.ofString(channel.getContent()));
            msg.add(FullBulkValueRedisMessage.ofString(message));
            return channels.stream().map(ch -> new PubPair(ch, new ListRedisMessage(msg))).collect(Collectors.toSet());
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

