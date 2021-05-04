package cn.deepmax.redis.integration;

import cn.deepmax.redis.RedisServer;
import cn.deepmax.redis.type.RedisString;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseTest {

    protected static RedisServer server;
    protected static RedisClient client;
    protected static StatefulRedisConnection<String, String> connection;
    protected static    RedisCommands<String, String> redis ;


    @BeforeClass
    public static void beforeClass() throws Exception {
        int port = 6380;
        server = new RedisServer(port);
        server.start();
        RedisURI uri = RedisURI.create("localhost", port);
        client = RedisClient.create(uri);
        connection = client.connect();

        redis  = connection.sync();


    }


    @AfterClass
    public static void afterClass() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (redis != null) {
            redis.shutdown(false);
        }
        if (client != null) {
            client.shutdown();
        }
    }
}
