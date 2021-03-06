package cn.deepmax.redis;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.resp3.MapRedisMessage;
import cn.deepmax.redis.resp3.NullRedisMessage;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 */
public class Network {

    public static RedisMessage nullValue(Client.Protocol protocol) {
        return protocol == Client.Protocol.RESP2 ? FullBulkValueRedisMessage.NULL_INSTANCE : NullRedisMessage.INSTANCE;
    }

    public static RedisMessage nullArray(Client client) {
        return client.isV2() ? ArrayRedisMessage.NULL_INSTANCE : NullRedisMessage.INSTANCE;
    }

    public static RedisMessage nullValue(Client client) {
        return nullValue(client.resp());
    }

    public static RedisMessage map(Client client, ListRedisMessage msg) {
        return client.isV2() ? msg : new MapRedisMessage(msg.children());
    }

}
