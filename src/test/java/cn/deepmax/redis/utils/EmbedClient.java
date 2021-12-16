package cn.deepmax.redis.utils;

import cn.deepmax.redis.core.NettyClient;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * @author wudi
 * @date 2021/12/16
 */
public class EmbedClient extends NettyClient {
    public EmbedClient() {
        super(new EmbeddedChannel());
    }
}
