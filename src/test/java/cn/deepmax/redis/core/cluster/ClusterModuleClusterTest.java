package cn.deepmax.redis.core.cluster;

import cn.deepmax.redis.base.BaseClusterTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.core.RedisTemplate;

import static org.junit.Assert.assertNotNull;

/**
 * @author wudi
 * @date 2022/1/12
 */
public class ClusterModuleClusterTest extends BaseClusterTemplateTest {
    public ClusterModuleClusterTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldInfo() {
        try (RedisClusterConnection con = t().getConnectionFactory().getClusterConnection()) {
            ClusterInfo info = con.clusterGetClusterInfo();
            assertNotNull(info);
        }
    }
}
