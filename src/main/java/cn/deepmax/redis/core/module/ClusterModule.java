package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisConfiguration;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.Statistic;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.CompositeCommand;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
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

    static int EPOCH = 0;

    public ClusterModule() {
        super("cluster");
        register(new CompositeCommand("Cluster")
                .with(new ClusterNodes())
                .with(new ClusterInfo())
                .with(new BaseClusterReplicas("clusterreplicas"))
                .with(new BaseClusterReplicas("clusterslaves"))
                .with(new ClusterSlots())
                .with(new ClusterMyId())
        );
        register("readonly", Constants.COMMAND_OK);
        register("readwrite", Constants.COMMAND_OK);
    }

    public static class ClusterMyId extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            Optional<RedisConfiguration.Node> node = client.node();
            if (node.isPresent()) {
                return FullBulkValueRedisMessage.ofString(node.get().id);
            } else {
                return Constants.ERR_NO_CLUSTER;
            }
        }
    }

    public static class ClusterInfo extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            if (!client.node().isPresent()) {
                return Constants.ERR_NO_CLUSTER;
            }
            RedisConfiguration.Cluster cluster = engine.configuration().getCluster();
            Statistic st = engine.statistic();
            StringBuilder sb = new StringBuilder();
            String content = sb.append("cluster_state:ok").append("\n")
                    .append("cluster_slots_assigned:16384").append("\n")
                    .append("cluster_slots_ok:16384").append("\n")
                    .append("cluster_slots_pfail:0").append("\n")
                    .append("cluster_slots_fail:0").append("\n")
                    .append("cluster_known_nodes:" + cluster.getAllNodes().size()).append("\n")
                    .append("cluster_size:" + cluster.getMasterNodes().size()).append("\n")
                    .append("cluster_current_epoch:" + EPOCH).append("\n")
                    .append("cluster_my_epoch:" + EPOCH).append("\n")
                    .append("cluster_stats_messages_sent:" + st.messageSend()).append("\n")
                    .append("cluster_stats_messages_received:" + st.messageRev()).append("\n")
                    .append("total_cluster_links_buffer_limit_exceeded:0").toString();
            return FullBulkValueRedisMessage.ofString(content);
        }
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
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            Optional<RedisConfiguration.Node> node = client.node();
            if (!node.isPresent()) {
                return Constants.ERR_NO_CLUSTER;
            }
            List<RedisConfiguration.Node> nodes = engine.configuration().getCluster().getAllNodes();
            return clusterNodeInfo(engine, node.get(), nodes);
        }
    }

    //<id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
    public static class BaseClusterReplicas extends ArgsCommand.ThreeEx {
        final String name;

        public BaseClusterReplicas(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            Optional<RedisConfiguration.Node> node = client.node();
            if (!node.isPresent()) {
                return Constants.ERR_NO_CLUSTER;
            }
            String id = msg.getAt(2).str();
            RedisConfiguration.Cluster cluster = engine.configuration().getCluster();
            Optional<RedisConfiguration.Node> master = cluster.getMasterNodes().stream().filter(m -> m.id.equalsIgnoreCase(id)).findFirst();
            if (!master.isPresent()) {
                return new ErrorRedisMessage("id is not a cluster master id");
            }
            List<RedisConfiguration.Node> nodes = cluster.getAllNodes().stream().filter(n ->
                    id.equalsIgnoreCase(n.masterId)
            ).collect(Collectors.toList());
            return clusterNodeInfo(engine, node.get(), nodes);
        }
    }

    static RedisMessage clusterNodeInfo(RedisEngine engine, RedisConfiguration.Node thisNode, List<RedisConfiguration.Node> nodes) {
        String SEP = " ";
        String NL = "\n";
        String serverHost = engine.configuration().getServerHost();
        long pongRev = System.currentTimeMillis();
        String str = nodes.stream().map(n -> {
            StringBuilder sb = new StringBuilder();
            sb.append(n.id).append(SEP);
            sb.append(serverHost).append(":").append(n.port).append("@").append((n.port + 10000)).append(SEP);
            if (n == thisNode) {
                sb.append("myself,");
            }
            sb.append(n.isMaster() ? "master" : "slave").append(SEP);
            sb.append(n.isMaster() ? "-" : n.getMasterId()).append(SEP);
            sb.append("0").append(SEP);
            sb.append(pongRev).append(SEP);
            sb.append(EPOCH).append(SEP);
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
