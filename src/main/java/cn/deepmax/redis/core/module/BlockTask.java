package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author wudi
 * @date 2021/12/27
 */
public class BlockTask {
    private final Redis.Client client;
    private final List<Key> keys;
    private final Long timeout;
    private final RedisEngine engine;
    private final Supplier<Optional<RedisMessage>> success;
    private final Supplier<RedisMessage> fail;
    private ScheduledFuture<?> future;

    public BlockTask(Redis.Client client, List<Key> keys, Long timeout, RedisEngine engine,
                     Supplier<Optional<RedisMessage>> success, Supplier<RedisMessage> fail) {
        this.client = client;
        this.keys = keys;
        this.timeout = timeout;
        this.engine = engine;
        this.success = success;
        this.fail = fail;
    }

    public void register() {
        DbManager.KeyEventListener outListener = (modified, listener) -> {
            Optional<RedisMessage> redisMessage = success.get();
            if (redisMessage.isPresent()) {
                client.channel().writeAndFlush(redisMessage.get());
                engine.getDbManager().removeListener(listener);
                if (this.future != null) {
                    this.future.cancel(true);
                }
            }
        };
        engine.getDbManager().addListener(client, keys, outListener);
        if (timeout > 0) {
            this.future = client.channel().eventLoop().schedule(() -> {
                engine.getDbManager().removeListener(outListener);
                client.channel().writeAndFlush(fail.get());
            }, timeout, TimeUnit.SECONDS);
        }
    }

}
