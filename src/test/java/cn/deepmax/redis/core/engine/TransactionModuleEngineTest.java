package cn.deepmax.redis.core.engine;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.base.BaseMemEngineTest;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.*;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TransactionModuleEngineTest extends BaseMemEngineTest {
    @Test
    public void shouldTxNornal() {
        ExpectedEvents evnets = listen(Arrays.asList("k", "k2"));
        Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("multi"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        msg = engine().execute(ListRedisMessage.ofString("set k 1"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("incrby k 10"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("get k"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("set k2 1"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "QUEUED");

        msg = engine().execute(ListRedisMessage.ofString("exec"), client);
        ListRedisMessage m = (ListRedisMessage) msg;
        assertEquals(m.children().size(), 4);
        assertEquals(((SimpleStringRedisMessage) m.children().get(0)).content(), "OK");
        assertEquals(((IntegerRedisMessage) m.children().get(1)).value(), 11L);
        assertEquals(((FullBulkValueRedisMessage) m.children().get(2)).str(), "11");
        assertEquals(evnets.triggerTimes,1);
        assertEquals(evnets.events.size(),2);
    }

    @Test
    public void shouldTxError() {
        Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        ExpectedEvents events = listen("k");
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
        assertEquals(events.triggerTimes,0);
    }

    @Test
    public void shouldTxError2() {
        Client client = embeddedClient();
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
        Client client = embeddedClient();
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
        Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        ExpectedEvents events = listen("k");
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
        assertEquals(events.triggerTimes,0);
    }

    @Test
    public void shouldTxDiscardError() {
        Client client = embeddedClient();
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("set k old"), client);
        assertEquals(((SimpleStringRedisMessage) msg).content(), "OK");

        ExpectedEvents events = listen("k");
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
        assertEquals(events.triggerTimes,0);
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
        Client client = embeddedClient();
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
        Client client = embeddedClient();
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
        Client client = embeddedClient();
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
        Client client = embeddedClient();
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
        Client client = embeddedClient();

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
        Client client = embeddedClient();

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
