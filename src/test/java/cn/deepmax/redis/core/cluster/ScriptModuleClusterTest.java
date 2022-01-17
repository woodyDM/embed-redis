package cn.deepmax.redis.core.cluster;

import cn.deepmax.redis.base.BaseClusterTemplateTest;
import cn.deepmax.redis.core.mixed.ScriptModuleMixedTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class ScriptModuleClusterTest extends BaseClusterTemplateTest {
    public ScriptModuleClusterTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldExec() {
        String sc = " redis.call('SET', KEYS[1], ARGV[1]) ; return 1; ";
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(sc, Long.class);
        Long execute = t().execute(redisScript, Collections.singletonList("name"), "myName你👌");

        assertEquals(execute.longValue(), 1L);
        assertEquals(v().get("name"), "myName你👌");
    }

    @Test
    public void shouldErrSetRespArg2() {
        expectMsg("redis.setresp() requires one argument");

        String sc = " redis.setresp('a','aa') ; return 1; ";
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(sc, Long.class);
        t().execute(redisScript,  Collections.singletonList("any"), "any");
    }

    @Test
    public void shouldErrSetRespArg0() {
        expectMsg("redis.setresp() requires one argument");

        String sc = " redis.setresp() ; return 1; ";
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(sc, Long.class);
        t().execute(redisScript,  Collections.singletonList("any"), "any");
    }

    @Test
    public void shouldErrSetRespArg1_Err() {
        expectMsg("RESP version must be 2 or 3.");

        String sc = " redis.setresp('4') ; return 1; ";
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(sc, Long.class);
        t().execute(redisScript, Collections.singletonList("any"), "any");
    }

    @Test
    public void shouldSetRespArg2() {
        String sc = " redis.setresp(2) ; return 1; ";
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(sc, Long.class);
        t().execute(redisScript, Collections.singletonList("any"), "any");
    }

    @Test
    public void shouldBrpop() {
        if (isLettuce()) {
            return;
        }
        String sc = " return redis.call('brpop','list',300) ";
        DefaultRedisScript<Object> redisScript = new DefaultRedisScript<>(sc, Object.class);
        Object result = t().execute(redisScript, Collections.singletonList("any"), "any");

        assertNull(result);
    }

    @Test
    public void shouldErrorReply() {
        expectMsg("some error");

        String sc = " return redis.error_reply('some error') ";
        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>(sc, String.class);
        String result = t().execute(redisScript, Collections.singletonList("any"), "any");
    }

    @Test
    public void shouldPCall() {


        String sc = "  redis.pcall('set','2') ; return redis.pcall('set','key','value'); ";
        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>(sc, String.class);

        try {
            t().execute(redisScript, Collections.singletonList("any"), "any");
        } catch (Exception e) {
            //ignore
        }
        //pcall will continue exec commands
        assertArrayEquals(get(bytes("key")), bytes("value"));

    }

    @Test
    public void shouldCall() {


        String sc = "  redis.call('set','2') ; return redis.call('set','key','value'); ";
        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>(sc, String.class);

        try {
            t().execute(redisScript, Collections.singletonList("any"), "any");
        } catch (Exception e) {
            //ignore
        }
        //call will not continue exec commands
        assertNull(get(bytes("key")));
    }

    @Test
    public void shouldSetResp3() {
        String sc = " redis.setresp(3) ; return 1; ";
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(sc, Long.class);
        t().execute(redisScript, Collections.singletonList("any"), "any");
    }

    @Test
    public void shouldEvalsha() {
        String script = "return ARGV[1];";
        RedisTemplate<String, Object> redisTemplate = t();
        DefaultRedisScript<byte[]> redisScript = new DefaultRedisScript<>(script, byte[].class);
        byte[] results = redisTemplate.execute(redisScript, Collections.singletonList("any"), new byte[]{5, 11, 2, 3});

        assertEquals(results[0], 5);
        assertEquals(results[1], 11);
        assertEquals(results[2], 2);
        assertEquals(results[3], 3);
    }

    /**
     * @see ScriptModuleMixedTest#shouldExec2()
     */
    @Test
    public void shouldExec2() {
        String sc = " redis.call('set', KEYS[1], ARGV[1]) ;" +
                " redis.call('lpush', KEYS[2], ARGV[2]) ; " +
                " local v = redis.call('get',KEYS[1]);" +
                " return v; ";

        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>(sc, String.class);
        String execute = t().execute(redisScript, Arrays.asList("key1{slot}", "key2{slot}"), "myName你👌", "list_value");

        assertEquals(execute, "myName你👌");
        assertEquals(v().get("key1{slot}"), "myName你👌");
        assertEquals(l().rightPop("key2{slot}"), "list_value");
    }

    private void expectMsg(String msg) {
        expectedException.expectMessage(msg);
    }
}
