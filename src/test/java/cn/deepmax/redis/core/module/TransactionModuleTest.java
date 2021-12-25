package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.base.BaseTemplateTest;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.*;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/24
 */
public class TransactionModuleTest extends BaseTemplateTest {
    public TransactionModuleTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldRunTx() {
        SessionCallback<Object> sessionCallback = new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                operations.opsForValue().set("k1", "old");
                operations.opsForValue().getAndSet("k1", "new");
                operations.opsForValue().set("k2", "v2", 15, TimeUnit.SECONDS);
                return operations.exec();
            }
        };
        //redisson v:  size = 1 : only old
        //jedis / lettuce: v: size = 3: true old true
        List<Object> v = (List<Object>) t().execute(sessionCallback);

        assertTrue(v.stream().anyMatch(vl -> "old".equals(vl)));
        assertEquals(v().get("k1"), "new");
        assertEquals(v().get("k2"), "v2");
        assertEquals(t().getExpire("k2").longValue(), 15L);
    }

    @Test
    public void shouldRunEmpty() {
        SessionCallback<Object> sessionCallback = new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                return operations.exec();
            }
        };
        List<Object> v = (List<Object>) t().execute(sessionCallback);

        assertEquals(v.size(), 0);
    }

    @Test
    public void shouldTxNornal() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 1"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incrby k 10"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);
        ListRedisMessage m = (ListRedisMessage) msg;
        assertEquals(m.children().size(), 3);
        assertEquals(((SimpleStringRedisMessage) m.children().get(0)).content(), "OK");
        assertEquals(((IntegerRedisMessage) m.children().get(1)).value(), 11L);
        assertEquals(((FullBulkValueRedisMessage) m.children().get(2)).str(), "11");
    }

    @Test
    public void shouldTxError() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incr k 10"), client);
        assertTrue(((ErrorRedisMessage) msg).content().contains("ERR wrong number of arguments for "));

        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);
        assertTrue(((ErrorRedisMessage) msg).content().contains("EXECABORT Transaction discarded because of previous errors."));

        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "old");
    }

    @Test
    public void shouldTxError2() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("somenot Now"), client);
        assertTrue(((ErrorRedisMessage) msg).content().contains("Embed-redis does not support this command"));

        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);
        assertTrue(((ErrorRedisMessage) msg).content().contains("EXECABORT Transaction discarded because of previous errors."));

        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "old");
    }

    @Test
    public void shouldTxWatchErrorButExec() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("watch k"), client);
        assertTrue(((ErrorRedisMessage) msg).content().contains("ERR WATCH inside MULTI is not allowed"));

        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        assertTrue(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("set k new"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);
        ListRedisMessage m = (ListRedisMessage) msg;
        assertEquals(m.children().size(), 3);
        assertEquals(((SimpleStringRedisMessage) m.children().get(0)).content(), "OK");
        assertEquals(((IntegerRedisMessage) m.children().get(1)).value(), 3L);
        assertEquals(((SimpleStringRedisMessage) m.children().get(2)).content(), "OK");

        assertFalse(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "new");
    }

    @Test
    public void shouldTxDiscardNoError() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incrby k 10"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");
        assertTrue(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("discard"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        assertFalse(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "old");
    }

    @Test
    public void shouldTxDiscardError() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incr k 10"), client);
        assertTrue(((ErrorRedisMessage) msg).content().contains("ERR wrong number of arguments for "));

        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        assertTrue(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("discard"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        assertFalse(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "old");
    }

    @Test
    public void shouldExecNotInTx() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("exec"), embeddedClient());

        assertTrue(msg instanceof ErrorRedisMessage);
        assertEquals(((ErrorRedisMessage) msg).content(), "ERR EXEC without MULTI");
    }

    @Test
    public void shouldDiscardNotInTx() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("discard"), embeddedClient());

        assertTrue(msg instanceof ErrorRedisMessage);
        assertEquals(((ErrorRedisMessage) msg).content(), "ERR DISCARD without MULTI");

    }

    /* Watch */
    @Test
    public void shouldWatchTxNornal() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("watch 1 2 3"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 1"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incrby k 10"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);
        ListRedisMessage m = (ListRedisMessage) msg;
        assertEquals(m.children().size(), 3);
        assertEquals(((SimpleStringRedisMessage) m.children().get(0)).content(), "OK");
        assertEquals(((IntegerRedisMessage) m.children().get(1)).value(), 11L);
        assertEquals(((FullBulkValueRedisMessage) m.children().get(2)).str(), "11");
    }

    /* Watch */
    @Test
    public void shouldWatchAndUnwatchTxNornal() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("watch 1 2 3"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 1"), embeddedClient());
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("unwatch"), embeddedClient());
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");
        
        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 1"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incrby k 10"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");
        //other set ,but no watch.
        msg = engine().execute(ListRedisMessage.ofString("set k new"), embeddedClient());
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");
        
        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);
        ListRedisMessage m = (ListRedisMessage) msg;
        assertEquals(m.children().size(), 3);
        assertEquals(((SimpleStringRedisMessage) m.children().get(0)).content(), "OK");
        assertEquals(((IntegerRedisMessage) m.children().get(1)).value(), 11L);
        assertEquals(((FullBulkValueRedisMessage) m.children().get(2)).str(), "11");
    }

    @Test
    public void shouldWatchTxError() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("watch k 2 3"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");
        //other client modify k
        engine().execute(ListRedisMessage.ofString("set k old-client2"), embeddedClient());
        assertTrue(client.queued());

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);

        assertSame(msg, FullBulkStringRedisMessage.NULL_INSTANCE);
        assertFalse(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "old-client2");
    }

    @Test
    public void shouldWatchTxErrorWatchSameValue() {
        Redis.Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("watch k 2 3"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("watch k 2 3 1 3 5"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");
        //other client set same value for key ,but expect fail
        engine().execute(ListRedisMessage.ofString("set k old"), embeddedClient());
        assertTrue(client.queued());

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);

        assertSame(msg, FullBulkStringRedisMessage.NULL_INSTANCE);
        assertFalse(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "old");
    }

    @Test
    public void shouldWatchTxErrorWatchNil() {
        Redis.Client client = embeddedClient();
        
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("watch k 2 3"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");
        //other client set same value for key ,but expect fail
        engine().execute(ListRedisMessage.ofString("set k old"), embeddedClient());
        assertTrue(client.queued());

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);

        assertSame(msg, FullBulkStringRedisMessage.NULL_INSTANCE);
        assertFalse(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "old");
    }
    
//    @Ignore("to fix this ,use new key modify events")
    @Test
    public void shouldWatchTxErrorWatchNilSetAndDel() {
        Redis.Client client = embeddedClient();

        RedisMessage msg = engine().execute(ListRedisMessage.ofString("watch k 2 3"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 2"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incr k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");
        //other client set and del value for key ,but expect fail
        engine().execute(ListRedisMessage.ofString("set k old"), embeddedClient());
        engine().execute(ListRedisMessage.ofString("del k"), embeddedClient());
        assertTrue(client.queued());

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);

        assertSame(msg, FullBulkStringRedisMessage.NULL_INSTANCE);
        assertFalse(client.queued());
        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertSame(FullBulkStringRedisMessage.NULL_INSTANCE, msg);
    }

}