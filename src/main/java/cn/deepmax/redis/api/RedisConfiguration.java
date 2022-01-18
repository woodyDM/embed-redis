package cn.deepmax.redis.api;

import java.util.*;

/**
 * @author wudi
 */
public class RedisConfiguration {

    private final String serverHost;
    private final Standalone standalone;
    private final Cluster cluster;

    public RedisConfiguration(String serverHost, Standalone standalone, Cluster cluster) {
        this.serverHost = serverHost;
        this.standalone = standalone;
        this.cluster = cluster;
    }

    public String getServerHost() {
        return serverHost;
    }

    public Standalone getStandalone() {
        return standalone;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void check() {
        if (standalone != null && cluster != null) {
            boolean overlay = cluster.getAllNodes().stream().anyMatch(n -> n.port == standalone.getPort());
            if (overlay) {
                throw new IllegalArgumentException("cluster port " + standalone.getPort()
                        + "should not same as standalone port");
            }
        } else if (standalone != null) {
            //no need to check standalone config
        } else if (cluster != null) {
            cluster.check();
        } else {
            throw new IllegalArgumentException("invalid redis server configuration");
        }

    }

    public static class Cluster {

        private static final int MAX_SLOT_IDX = (1 << 14) - 1;
        private final String auth;
        private final List<Node> nodes;
        private List<Node> allNodes;

        public Cluster(String auth, List<Node> masterNodes) {
            Objects.requireNonNull(masterNodes);
            this.nodes = new ArrayList<>(masterNodes);
            this.auth = auth;
            this.useDefaultSlot();
        }

        public void useDefaultSlot() {
            int masters = nodes.size();
            int per = (MAX_SLOT_IDX + 1) / masters;
            for (int i = 0; i < masters; i++) {
                SlotRange slot = new SlotRange();
                slot.start = i * per;
                if (i != masters - 1) {
                    slot.end = (i + 1) * per - 1;
                } else {
                    slot.end = MAX_SLOT_IDX;
                }
                nodes.get(i).slotRange = slot;
            }
        }

        public void check() {
            if (nodes.size() == 1) {
                return;
            }
            long c1 = nodes.stream().map(n -> n.port).distinct().count();
            if (c1 != nodes.size()) {
                throw new IllegalArgumentException("port has same value");
            }
            long c2 = nodes.stream().map(n -> n.name).distinct().count();
            if (c2 != nodes.size()) {
                throw new IllegalArgumentException("node name has same value");
            }
        }

        public String getAuth() {
            return auth;
        }

        public List<Node> getMasterNodes() {
            return nodes;
        }

        public synchronized List<Node> getAllNodes() {
            if (allNodes == null) {
                allNodes = new ArrayList<>();
                for (Node node : nodes) {
                    allNodes.add(node);
                    allNodes.addAll(node.getSlaves());
                }
            }
            return Collections.unmodifiableList(allNodes);
        }
    }

    public static class SlotRange {
        int start;
        int end;

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }
    }

    public static class Node {
        public final String name;
        public final String id;
        public final int port;
        public String masterId;
        private List<Node> slaves;
        private SlotRange slotRange;

        public Node(String name, int port) {
            this.name = name;
            this.port = port;
            this.id = randStr(32) + randStr(8);
        }

        //use uuid for simple
        private static String randStr(int s) {
            return UUID.randomUUID().toString().replace("-", "").toLowerCase().substring(0, s);
        }

        public SlotRange getSlotRange() {
            return slotRange;
        }

        public boolean isMaster() {
            return slaves != null;
        }

        public Node appendSlave(Node slave) {
            if (slaves == null) slaves = new ArrayList<>();
            slave.masterId = this.id;
            slaves.add(slave);
            return this;
        }

        public String getMasterId() {
            return masterId;
        }

        public List<Node> getSlaves() {
            return slaves == null ? Collections.emptyList() : Collections.unmodifiableList(slaves);
        }

    }

    public static class Standalone {

        private int port = 6379;
        private String auth;

        public Standalone(int port, String auth) {
            this.port = port;
            this.auth = auth;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getAuth() {
            return auth;
        }

        public void setAuth(String auth) {
            this.auth = auth;
        }
    }

}
