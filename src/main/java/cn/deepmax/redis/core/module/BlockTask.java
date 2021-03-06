package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.DbManager;
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
 */
public class BlockTask {
    private final Client client;
    private final List<Key> keys;
    private final Long timeout; //mills
    private final RedisEngine engine;
    private final Supplier<Optional<RedisMessage>> success;
    private final Supplier<RedisMessage> fail;
    private ScheduledFuture<?> future;

    public BlockTask(Client client, List<Key> keys, Long timeout, RedisEngine engine,
                     Supplier<Optional<RedisMessage>> success, Supplier<RedisMessage> fail) {
        this.client = client;
        this.keys = keys;
        this.timeout = timeout;
        this.engine = engine;
        this.success = success;
        this.fail = fail;
    }

    public void block() {
        DbManager.KeyEventListener outListener = (modified, listener) -> {
            Optional<RedisMessage> redisMessage = success.get();
            if (redisMessage.isPresent()) {
                client.pub(redisMessage.get());
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
                client.pub(fail.get());
            }, timeout, TimeUnit.MILLISECONDS);
        }
    }

}
