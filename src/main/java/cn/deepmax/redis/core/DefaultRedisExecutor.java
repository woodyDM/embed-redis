package cn.deepmax.redis.core;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.module.ConnectionModule;
import cn.deepmax.redis.utils.MessagePrinter;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wudi
 * @date 2021/5/8
 */
@Slf4j
public class DefaultRedisExecutor implements RedisExecutor {
    static Set<String> authWhiteList = new HashSet<>();
    static Set<String> txWhiteList = new HashSet<>();

    private final AtomicLong requestCounter = new AtomicLong();
    private final AtomicLong responseCounter = new AtomicLong();
    private final DefaultStatistic statistic = new DefaultStatistic();

    static {
        authWhiteList.add("hello");
        authWhiteList.add("ping");
        txWhiteList.add("exec");
        txWhiteList.add("reset");
    }

    /**
     * @return
     */
    public Statistic statistic() {
        return statistic;
    }

    /**
     * execute
     *
     * @param type
     * @param engine
     * @param client
     * @return
     */
    @Override
    public RedisMessage execute(RedisMessage type, RedisEngine engine, Client client) {
        Objects.requireNonNull(client);
        MessagePrinter.requestStart(client, requestCounter.getAndIncrement());
        MessagePrinter.printMessage(type, client.queued());
        return doExec(type, engine, client);
    }

    private RedisMessage doExec(RedisMessage type, RedisEngine engine, Client client) {
        AuthManager auth = engine.authManager();
        RedisCommand command = engine.commandManager().getCommand(type);
        String cmdName = command.name().toLowerCase();

        if (auth.needAuth() && !auth.alreadyAuth(client) && !authWhiteList.contains(cmdName)) {
            command = wrapAuth(command);
        } else if (client.queued() && !txWhiteList.contains(cmdName)) {
            command = wrapTx(command);
        }
        RedisMessage response;
        try {
            response = command.response(type, client, engine);
            //executor will not fire event in transaction or scripting.
            if (!client.queued() && !client.queryFlag(Client.FLAG_SCRIPTING) && !client.queryFlag(Client.FLAG_QUEUE_EXEC)) {
                engine.getDbManager().fireChangeQueuedEvents(client);
            }
        } catch (RedisServerException e) {
            response = e.getMsg() == null ? new ErrorRedisMessage(e.getMessage()) : e.getMsg();
        } catch (Exception e) {
            response = new ErrorRedisMessage("ERR internal redis server error!");
            log.error("Embed server error, may be bug! ", e);
        }
        MessagePrinter.responseStart(client, responseCounter.getAndIncrement());
        MessagePrinter.printMessage(response, client.queued());
        return response;
    }

    private RedisCommand wrapTx(RedisCommand command) {
        return ((msg, client, en) -> en.transactionManager().queue(client, msg));
    }

    /**
     * wrap for auth
     *
     * @param command
     * @return
     */
    private RedisCommand wrapAuth(RedisCommand command) {
        return ((type, client, en) -> {
            if (command instanceof ConnectionModule.Auth ||
                    command == Constants.UNKNOWN_COMMAND ||
                    en.authManager().alreadyAuth(client)) {
                return command.response(type, client, en);
            } else {
                return Constants.ERR_NO_AUTH;
            }
        });
    }

    class DefaultStatistic implements Statistic {
        @Override
        public long messageRev() {
            return requestCounter.get();
        }

        @Override
        public long messageSend() {
            return responseCounter.get();
        }

        @Override
        public long incrSend() {
            return responseCounter.getAndIncrement();
        }
    }
}

