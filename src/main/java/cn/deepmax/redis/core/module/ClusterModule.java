package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.CompositeCommand;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2022/1/10
 */
public class ClusterModule extends BaseModule {
    public ClusterModule() {
        super("cluster");
        register(new CompositeCommand("Cluster")
                .with(new ClusterNodes())
                .with(new ClusterSlots()));
        register("readonly", Constants.COMMAND_OK);
    }
    
    public static class ClusterSlots extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            RedisConfiguration.Cluster cluster = engine.configuration().getCluster();
            if (cluster == null) {
                return Constants.ERR_NO_CLUSTER;
            }
            String serverHost = engine.configuration().getServerHost();
            ListRedisMessage.Builder builder = ListRedisMessage.newBuilder();
            for (RedisConfiguration.Node master : cluster.getMasterNodes()) {
                ListRedisMessage.Builder one = ListRedisMessage.newBuilder();
                one.append(new IntegerRedisMessage(master.getSlotRange().getStart()))
                        .append(new IntegerRedisMessage(master.getSlotRange().getEnd()));
                ListRedisMessage.Builder masterB = ListRedisMessage.newBuilder()
                        .append(FullBulkValueRedisMessage.ofString(serverHost))
                        .append(new IntegerRedisMessage(master.port))
                        .append(FullBulkValueRedisMessage.ofString(master.id));
                one.append(masterB.build());
                for (RedisConfiguration.Node slave : master.getSlaves()) {
                    ListRedisMessage.Builder slaveB = ListRedisMessage.newBuilder();
                    slaveB.append(FullBulkValueRedisMessage.ofString(serverHost))
                            .append(new IntegerRedisMessage(slave.port))
                            .append(FullBulkValueRedisMessage.ofString(slave.id));
                    one.append(slaveB.build());
                }
                builder.append(one.build());
            }
            return builder.build();
        }
    }

    //<id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
    public static class ClusterNodes extends ArgsCommand.TwoEx {
        String SEP = " ";
        String NL = "\n";

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            Optional<RedisConfiguration.Node> node = client.node();
            if (!node.isPresent()) {
                return Constants.ERR_NO_CLUSTER;
            }
            List<RedisConfiguration.Node> nodes = engine.configuration().getCluster().getAllNodes();
            String serverHost = engine.configuration().getServerHost();
            long pongRev = System.currentTimeMillis();
            String str = nodes.stream().map(n -> {
                StringBuilder sb = new StringBuilder();
                sb.append(n.id).append(SEP);
                sb.append(serverHost).append(":").append(n.port).append("@").append((n.port + 10000)).append(SEP);
                if (n == node.get()) {
                    sb.append("myself,");
                }
                sb.append(n.isMaster() ? "master" : "slave").append(SEP);
                sb.append(n.isMaster() ? "-" : n.getMasterId()).append(SEP);
                sb.append("0").append(SEP);
                sb.append(pongRev).append(SEP);
                sb.append(0).append(SEP);
                sb.append("connected");
                if (n.isMaster()) {
                    sb.append(SEP);
                    sb.append(n.getSlotRange().getStart()).append("-").append(n.getSlotRange().getEnd());
                }
                return sb.toString();
            }).collect(Collectors.joining(NL));
            return FullBulkValueRedisMessage.ofString(str);
        }
    }

}
