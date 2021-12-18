package cn.deepmax.redis.core;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.AuthManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisParamException;
import cn.deepmax.redis.core.module.AuthModule;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.type.CallbackRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import cn.deepmax.redis.type.RedisMessages;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wudi
 * @date 2021/5/8
 */
@Slf4j
public class DefaultRedisExecutor implements RedisExecutor {
    static Set<String> whiteList = new HashSet<>();

    static {
        whiteList.add("hello");
        whiteList.add("ping");
    }

    private final AtomicLong requestCounter = new AtomicLong();
    private final AtomicLong responseCounter = new AtomicLong();

    /**
     * execute
     *
     * @param type
     * @param engine
     * @param client
     * @return
     */
    @Override
    public RedisMessage execute(RedisMessage type, RedisEngine engine, Redis.Client client) {
        Objects.requireNonNull(client);
        log.debug("[{}]Request", requestCounter.getAndIncrement());
        printRedisMessage(type);
        return doExec(type, engine, client);
    }

    private RedisMessage doExec(RedisMessage type, RedisEngine engine, Redis.Client client) {
        AuthManager auth = engine.authManager();
        RedisCommand command = ((DefaultRedisEngine) engine).getCommandManager().getCommand(type);
        String cmdName = command.name();

        if (auth.needAuth() && !auth.alreadyAuth(client) && !whiteList.contains(cmdName.toLowerCase())) {
            command = wrapAuth(command);
        }
        RedisMessage response;
        try {
            response = command.response(type, client, engine);
        } catch (RedisParamException e) {
            response = new ErrorRedisMessage(e.getMessage());
        } catch (Exception e) {
            response = new ErrorRedisMessage("ERR internal server error");
            log.error("Embed server error, may be bug! ", e);
        }
        log.debug("[{}]Response", responseCounter.getAndIncrement());
        printRedisMessage(response);
        return response;
    }


    /**
     * wrap for auth
     *
     * @param command
     * @return
     */
    private RedisCommand wrapAuth(RedisCommand command) {
        return ((type, client, en) -> {
            if (command instanceof AuthModule.Auth ||
                    command == CommandManager.UNKNOWN_COMMAND ||
                    en.authManager().alreadyAuth(client)) {
                return command.response(type, client, en);
            } else {
                return Constants.NO_AUTH_ERROR;
            }
        });
    }


    private void printRedisMessage(RedisMessage msg) {
        doPrint(msg, 0, false);
    }

    private void doPrint(RedisMessage msg, int depth, boolean isLast) {
        if (msg == null) {
            return;
        }
        if (msg instanceof CallbackRedisMessage) {
            msg = ((CallbackRedisMessage) msg).unwrap();
        }
        String word = "";
        String space = String.join("", Collections.nCopies(depth, " "));
        if (RedisMessages.isError(msg)) {
            word = RedisMessages.getStr(msg);
        } else if (RedisMessages.isStr(msg)) {
            if (msg instanceof FullBulkValueRedisMessage) {
                word = msg.toString();
            } else {
                word = RedisMessages.getStr(msg);
            }
        } else if (msg instanceof IntegerRedisMessage) {
            word = "" + ((IntegerRedisMessage) msg).value();
        } else if (msg instanceof ArrayRedisMessage) {
            log.debug("{}-[{}] ", space,
                    msg.getClass().getSimpleName());
            List<RedisMessage> children = ((ArrayRedisMessage) msg).children();
            for (int i = 0; i < children.size(); i++) {
                doPrint(children.get(i), depth + 1, i == children.size() - 1);
            }
            return;
        } else if(msg instanceof CompositeRedisMessage){
            log.debug("{}-[{}] ", space,
                    msg.getClass().getSimpleName());
            List<RedisMessage> children = ((CompositeRedisMessage) msg).children();
            for (int i = 0; i < children.size(); i++) {
                doPrint(children.get(i), depth + 1, i == children.size() - 1);
            }
            return;
        }else {
            throw new CodecException("unknown message type: " + msg);
        }
        String corner = (isLast ? "└" : "├");
        log.debug("{}{}-[{}]{}", corner, space,
                msg.getClass().getSimpleName(), word);

    }
}

