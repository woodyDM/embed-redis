package cn.deepmax.redis.engine;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.engine.module.AuthModule;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisType;
import io.netty.handler.codec.CodecException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
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
    private final AtomicLong requestCounter = new AtomicLong();
    private final AtomicLong responseCounter = new AtomicLong();

    static Set<String> whiteList = new HashSet<>();

    static {
        whiteList.add("hello");
        whiteList.add("ping");
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
    public RedisType execute(RedisType type, RedisEngine engine, Redis.Client client) {
        Objects.requireNonNull(client);
        log.debug("[{}]Request", requestCounter.getAndIncrement());
        printRedisMessage(type);
        return doExec(type, engine, client);
    }
    
    private RedisType doExec(RedisType type, RedisEngine engine, Redis.Client client) {
        AuthManager auth = engine.authManager();
        RedisCommand command = ((DefaultRedisEngine) engine).getCommandManager().getCommand(type);
        String cmdName = command.name();

        if (auth.needAuth() && !auth.alreadyAuth(client) && !whiteList.contains(cmdName.toLowerCase())) {
            command = wrapAuth(command);
        }
        RedisType response;
        try {

            response = command.response(type, client, engine);
        } catch (RedisParamException e) {
            response = new RedisError(e.getMessage());
        } catch (Exception e) {
            response = new RedisError("ERR internal server error");
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


    private void printRedisMessage(RedisType msg) {
        doPrint(msg, 0, false);
    }

    private void doPrint(RedisType msg, int depth, boolean isLast) {
        String word = "";

        String space = String.join("", Collections.nCopies(depth, " "));
        if (msg.isString()) {
            word = msg.str();
        } else if (msg.isError()) {
            word = msg.str();
        } else if (msg.isInteger()) {
            word = "" + msg.value();
        } else if (msg.isArray() || msg.isComposite()) {
            log.debug("{}-[{}] ", space,
                    msg.getClass().getSimpleName());
            for (int i = 0; i < msg.children().size(); i++) {
                doPrint(msg.children().get(i), depth + 1, i == msg.children().size() - 1);
            }
            return;
        } else {
            throw new CodecException("unknown message type: " + msg);
        }
        String corner = (isLast ? "└" : "├");
        log.debug("{}{}-[{}]{}", corner, space,
                msg.getClass().getSimpleName(), word);

    }
}

