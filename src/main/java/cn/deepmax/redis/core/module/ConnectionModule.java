package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.Network;
import cn.deepmax.redis.api.AuthManager;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.CompositeCommand;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.type.CallbackRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * @author wudi
 * @date 2021/5/10
 */
@Slf4j
public class ConnectionModule extends BaseModule {
    public ConnectionModule() {
        super("connection");
        register(new Echo());
        register(new Reset());
        register(new Auth());
        register(new Hello());
        register(new Ping());
        register(new Quit());
        register(new Select());
        register(new FlushAll());
        register(new CompositeCommand("Client")
                .with(new ClientId())
                .with(new ClientSetName())
                .with(new ClientGetName())

        );
    }

    public static class ClientId extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            return new IntegerRedisMessage(client.id());
        }
    }

    public static class ClientSetName extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] name = msg.getAt(2).bytes();
            client.setName(name);
            return OK;
        }
    }

    public static class ClientGetName extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            return FullBulkValueRedisMessage.ofString(client.getName());
        }
    }

    public static class Auth extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            if (msg.children().size() > 2) {
                return new ErrorRedisMessage("Redis6 ACL is not supported");
            }
            AuthManager auth = engine.authManager();
            String userAuth = msg.getAt(1).str();
            if (!auth.needAuth() || auth.tryAuth(userAuth, client)) {
                return OK;
            } else {
                return new ErrorRedisMessage("WRONGPASS invalid username-password pair");
            }
        }

    }

    /**
     * 1# "server" => "redis"
     * 2# "version" => "6.0.0"
     * 3# "proto" => (integer) 3
     * 4# "id" => (integer) 10
     * 5# "mode" => "standalone"
     * 6# "role" => "master"
     * 7# "modules" => (empty array)
     */
    private static class Hello extends ArgsCommand<ArgsCommand.RVoid> {

        public Hello() {
            super(1, 2, 5, 4, 7);
        }

        //HELLO [protover [AUTH username password] [SETNAME clientname]]
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String version = "2";
            String auth = null;
            byte[] clientName = null;
            int size = msg.children().size();
            if (size > 1) {
                version = msg.getAt(1).str();
                if (!"2".equalsIgnoreCase(version) && !"3".equalsIgnoreCase(version)) {
                    return Constants.ERR_SYNTAX;
                }
                int pos = 2; //pointer 
                while (pos < size) {
                    String cm = msg.getAt(pos).str();
                    if ("auth".equalsIgnoreCase(cm)) {
                        if (pos + 2 < size) {
                            auth = msg.getAt(pos + 2).str();
                            pos += 3;
                        } else {
                            return Constants.ERR_SYNTAX;
                        }
                    } else if ("setname".equalsIgnoreCase(cm)) {
                        if (pos + 1 < size) {
                            clientName = msg.getAt(pos + 1).bytes();
                            pos += 2;
                        } else {
                            return Constants.ERR_SYNTAX;
                        }
                    } else {
                        return Constants.ERR_SYNTAX;
                    }
                }
                if (pos != size) return Constants.ERR_SYNTAX;
            }

            AuthManager authManager = engine.authManager();
            if (auth != null) {
                boolean ok = authManager.tryAuth(auth, client);
                if (!ok) {
                    return new ErrorRedisMessage("WRONGPASS invalid username-password pair");
                }
            } else {
                if (authManager.needAuth() && !authManager.alreadyAuth(client)) {
                    return new ErrorRedisMessage("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
                }
            }
            if (clientName != null) {
                client.setName(clientName);
            }
            client.setProtocol("2".equalsIgnoreCase(version) ? Client.Protocol.RESP2 : Client.Protocol.RESP3);
            Optional<RedisConfiguration.Node> node = client.node();
            ListRedisMessage result = ListRedisMessage.newBuilder()
                    .append("server")
                    .append("redis")
                    .append("version")
                    .append("6.2.0")
                    .append("proto")
                    .append(new IntegerRedisMessage(Integer.parseInt(version)))
                    .append("id")
                    .append(new IntegerRedisMessage(client.id()))
                    .append("mode")
                    .append(node.isPresent() ? "cluster" : "standalone")
                    .append("role")
                    .append(node.filter(n -> !n.isMaster()).isPresent() ? "slave" : "master")
                    .append("modules")
                    .append(ListRedisMessage.empty())
                    .build();
            return Network.map(client, result);
        }
    }

    private static class Ping extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            return new SimpleStringRedisMessage("PONG");
        }
    }

    public static class Echo extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            return FullBulkValueRedisMessage.ofString(msg.getAt(1).str());
        }
    }

    public static class Reset extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            client.setProtocol(Client.Protocol.RESP2);
            engine.authManager().clearAuth(client);
            engine.getDbManager().switchTo(client, 0);
            engine.transactionManager().unwatch(client);
            engine.pubsub().quit(client);
            client.setQueue(false);
            return Constants.RESET;
        }
    }

    private static class Quit extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage type, Client client, RedisEngine engine) {
            CallbackRedisMessage msg = CallbackRedisMessage.of(OK);
            msg.addHook(c ->
                    client.channel().close()
                            .addListener(e -> log.debug("Client quit! {}", client.channel().remoteAddress())));
            return msg;
        }

    }

    public static class FlushAll extends ArgsCommand.OneEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage type, Client client, RedisEngine engine) {
            engine.flush();
            return OK;
        }
    }

    private static class Select extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String idx = msg.getAt(1).str();
            int i = Integer.parseInt(idx);
            engine.getDbManager().switchTo(client, i);
            return OK;
        }
    }

}
