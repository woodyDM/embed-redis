package cn.deepmax.redis.core;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.AuthManager;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.module.ConnectionModule;
import cn.deepmax.redis.utils.MessagePrinter;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author wudi
 * @date 2021/5/8
 */
@Slf4j
public class DefaultRedisExecutor implements RedisExecutor {
    static Set<String> authWhiteList = new HashSet<>();
    static Set<String> txWhiteList = new HashSet<>();

    static {
        authWhiteList.add("hello");
        authWhiteList.add("ping");
        txWhiteList.add("exec");
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
        MessagePrinter.requestStart();
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
            engine.getDbManager().fireChangeQueuedEvents(client);
        } catch (RedisServerException e) {
            response = e.getMsg() == null ? new ErrorRedisMessage(e.getMessage()) : e.getMsg();
        } catch (Exception e) {
            response = new ErrorRedisMessage("ERR internal redis server error!");
            log.error("Embed server error, may be bug! ", e);
        }
        MessagePrinter.responseStart();
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

}

