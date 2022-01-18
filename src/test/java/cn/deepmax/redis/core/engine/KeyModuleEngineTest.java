package cn.deepmax.redis.core.engine;

import cn.deepmax.redis.base.BaseMemEngineTest;
import cn.deepmax.redis.resp3.ListRedisMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author wudi
 */
public class KeyModuleEngineTest extends BaseMemEngineTest {

    @Test
    public void shouldScanNormalRaw() {
        set("a", "1");
        set("b", "2");
        set("c", "3");
        rpush("d", "5");
        set("e", "4");
        rpush("f", "6");

        del("c");
        set("e", "44");

        ListRedisMessage msg;
        ListRedisMessage keys;

        msg = (ListRedisMessage) engine().execute(ListRedisMessage.ofString("scan 0 count 2"), embeddedClient());
        assertEquals(msg.getAt(0).str(), "4");
        keys = (ListRedisMessage) msg.children().get(1);
        assertEquals(keys.children().size(), 2);
        assertEquals(keys.getAt(0).str(), "a");
        assertEquals(keys.getAt(1).str(), "b");

        msg = (ListRedisMessage) engine().execute(ListRedisMessage.ofString("scan 4 count 2"), embeddedClient());
        assertEquals(msg.getAt(0).str(), "7");
        keys = (ListRedisMessage) msg.children().get(1);
        assertEquals(keys.children().size(), 2);
        assertEquals(keys.getAt(0).str(), "d");
        assertEquals(keys.getAt(1).str(), "f");

        msg = (ListRedisMessage) engine().execute(ListRedisMessage.ofString("scan 7 count 2"), embeddedClient());
        assertEquals(msg.getAt(0).str(), "0");
        keys = (ListRedisMessage) msg.children().get(1);
        assertEquals(keys.children().size(), 1);
        assertEquals(keys.getAt(0).str(), "e");
    }

    @Test
    public void shouldScanNormalRawPattern() {
        set("key-a", "1");
        set("key-b", "2");
        set("key-c", "3");
        rpush("key-d", "5");
        set("key-e", "4");
        set("k-n","5");
        set("k-m","6");
        rpush("f", "7");

        del("key-c");
        set("key-e", "44");
        //key-a key-b key-d k-n k-m f  key-e
        //1     2     4     6   7   8  9
        ListRedisMessage msg;
        ListRedisMessage keys;

        msg = (ListRedisMessage) engine().execute(ListRedisMessage.ofString("scan 0 count 2 match key-*"), embeddedClient());
        assertEquals(msg.getAt(0).str(), "4");
        keys = (ListRedisMessage) msg.children().get(1);
        assertEquals(keys.children().size(), 2);
        assertEquals(keys.getAt(0).str(), "key-a");
        assertEquals(keys.getAt(1).str(), "key-b");

        msg = (ListRedisMessage) engine().execute(ListRedisMessage.ofString("scan 4 count 2 match key-*"), embeddedClient());
        assertEquals(msg.getAt(0).str(), "7");
        keys = (ListRedisMessage) msg.children().get(1);
        assertEquals(keys.children().size(), 1);
        assertEquals(keys.getAt(0).str(), "key-d");
         

        msg = (ListRedisMessage) engine().execute(ListRedisMessage.ofString("scan 7 count 2 match key-*"), embeddedClient());
        assertEquals(msg.getAt(0).str(), "9");
        keys = (ListRedisMessage) msg.children().get(1);
        assertEquals(keys.children().size(), 0);
        
        msg = (ListRedisMessage) engine().execute(ListRedisMessage.ofString("scan 9 count 2 match key-*"), embeddedClient());
        assertEquals(msg.getAt(0).str(), "0");
        keys = (ListRedisMessage) msg.children().get(1);
        assertEquals(keys.children().size(), 1);
        assertEquals(keys.getAt(0).str(), "key-e");
    }

}