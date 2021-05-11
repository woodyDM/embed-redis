package cn.deepmax.redis.utils;

import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class RegexUtilsTest {

    @Test
    public void shouldToRegex() {
        assertEquals(RegexUtils.toRegx("?"),"^.{1}$");
        assertEquals(RegexUtils.toRegx("\\?"),"^\\?$");
        assertEquals(RegexUtils.toRegx("1?"),"^1.{1}$");
        assertEquals(RegexUtils.toRegx("?22"),"^.{1}22$");
        assertEquals(RegexUtils.toRegx("222"),"^222$");
    }


    @Test
    public void shouldSingle() {
        assertTrue(Pattern.compile(RegexUtils.toRegx("?")).matcher("1").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("?")).matcher("2").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("?")).matcher("?").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("?")).matcher("*").matches());
        
        assertFalse(Pattern.compile(RegexUtils.toRegx("?")).matcher("12").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("?")).matcher("").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("?")).matcher("123").matches());
        
    }

    @Test
    public void shouldMid() {
        assertTrue(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("t1st").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("test").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("tast").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("t?st").matches());

        assertFalse(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("tast1").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("1tast").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("tst").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("test5").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("t?st")).matcher("tesa").matches());

    }

    @Test
    public void shouldTail() {
        assertTrue(Pattern.compile(RegexUtils.toRegx("tes?")).matcher("test").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("tes?")).matcher("tesa").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("tes?")).matcher("tes?").matches());

        assertFalse(Pattern.compile(RegexUtils.toRegx("tes?")).matcher("tes?1").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("tes?")).matcher("test1").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("tes?")).matcher("tes").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("tes?")).matcher("").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("tes?")).matcher("tesa1").matches());
    }


    @Test
    public void shouldSingle2() {
        assertTrue(Pattern.compile(RegexUtils.toRegx("*")).matcher("1").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("*")).matcher("2").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("*")).matcher("?").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("*")).matcher("*").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("*")).matcher("12").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("*")).matcher("12345").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("*")).matcher("abcde").matches());
    }

    @Test
    public void shouldMid2() {
        assertTrue(Pattern.compile(RegexUtils.toRegx("t*st")).matcher("tst").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("t*st")).matcher("test").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("t*st")).matcher("teeeest").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("t*st")).matcher("teaaaeeest").matches());
        
        assertFalse(Pattern.compile(RegexUtils.toRegx("t*st")).matcher("tats").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("t*st")).matcher("teeeest2").matches());
    }

    @Test
    public void shouldTail2() {
        assertTrue(Pattern.compile(RegexUtils.toRegx("tes*")).matcher("tes").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("tes*")).matcher("test").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("tes*")).matcher("testttt?").matches());
        
        assertFalse(Pattern.compile(RegexUtils.toRegx("tes*")).matcher("teaa").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("tes*")).matcher("1test").matches());
    }

    @Test
    public void shouldSpecial() {
        assertTrue(Pattern.compile(RegexUtils.toRegx("te\\?*t")).matcher("te?12t").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("te\\?*t")).matcher("te?t").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("te\\?*t")).matcher("te?abct").matches());
        assertTrue(Pattern.compile(RegexUtils.toRegx("te\\?*")).matcher("te?abct").matches());
        
        assertFalse(Pattern.compile(RegexUtils.toRegx("te\\?*")).matcher("1te?abct").matches());
        assertFalse(Pattern.compile(RegexUtils.toRegx("te\\?*")).matcher("te.abct").matches());
    }
}