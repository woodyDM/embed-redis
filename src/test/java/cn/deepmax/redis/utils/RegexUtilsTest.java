package cn.deepmax.redis.utils;

import cn.deepmax.redis.core.RPattern;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * @author wudi
 */
public class RegexUtilsTest {

    @Test
    public void shouldToRegex() {
        assertEquals(RPattern.toRegx("?"), "^.{1}$");
        assertEquals(RPattern.toRegx("\\?"), "^\\?$");
        assertEquals(RPattern.toRegx("1?"), "^1.{1}$");
        assertEquals(RPattern.toRegx("?22"), "^.{1}22$");
        assertEquals(RPattern.toRegx("222"), "^222$");
    }

    @Test
    public void shouldSingle() {
        assertTrue(Pattern.compile(RPattern.toRegx("?")).matcher("1").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("?")).matcher("2").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("?")).matcher("?").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("?")).matcher("*").matches());

        assertFalse(Pattern.compile(RPattern.toRegx("?")).matcher("12").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("?")).matcher("").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("?")).matcher("123").matches());

    }

    @Test
    public void shouldMid() {
        assertTrue(Pattern.compile(RPattern.toRegx("t?st")).matcher("t1st").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("t?st")).matcher("test").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("t?st")).matcher("tast").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("t?st")).matcher("t?st").matches());

        assertFalse(Pattern.compile(RPattern.toRegx("t?st")).matcher("tast1").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("t?st")).matcher("1tast").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("t?st")).matcher("").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("t?st")).matcher("tst").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("t?st")).matcher("test5").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("t?st")).matcher("tesa").matches());

    }

    @Test
    public void shouldTail() {
        assertTrue(Pattern.compile(RPattern.toRegx("tes?")).matcher("test").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("tes?")).matcher("tesa").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("tes?")).matcher("tes?").matches());

        assertFalse(Pattern.compile(RPattern.toRegx("tes?")).matcher("tes?1").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("tes?")).matcher("test1").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("tes?")).matcher("tes").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("tes?")).matcher("").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("tes?")).matcher("tesa1").matches());
    }


    @Test
    public void shouldSingle2() {
        assertTrue(Pattern.compile(RPattern.toRegx("*")).matcher("1").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("*")).matcher("2").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("*")).matcher("?").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("*")).matcher("*").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("*")).matcher("12").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("*")).matcher("12345").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("*")).matcher("abcde").matches());
    }

    @Test
    public void shouldMid2() {
        assertTrue(Pattern.compile(RPattern.toRegx("t*st")).matcher("tst").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("t*st")).matcher("test").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("t*st")).matcher("teeeest").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("t*st")).matcher("teaaaeeest").matches());

        assertFalse(Pattern.compile(RPattern.toRegx("t*st")).matcher("tats").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("t*st")).matcher("teeeest2").matches());
    }

    @Test
    public void shouldTail2() {
        assertTrue(Pattern.compile(RPattern.toRegx("tes*")).matcher("tes").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("tes*")).matcher("test").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("tes*")).matcher("testttt?").matches());

        assertFalse(Pattern.compile(RPattern.toRegx("tes*")).matcher("teaa").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("tes*")).matcher("1test").matches());
    }

    @Test
    public void shouldNeg() {
        assertTrue(Pattern.compile(RPattern.toRegx("h[^e]llo")).matcher("hallo").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("h[^e]llo")).matcher("hbllo").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("h[^e]llo")).matcher("hello").matches());
    }

    @Test
    public void shouldAll() {
        assertTrue(Pattern.compile(RPattern.toRegx("h[a-d]llo")).matcher("hallo").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("h[a-d]llo")).matcher("hdllo").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("h[a-d]llo")).matcher("hello").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("h[a-d]llo")).matcher("hallo2").matches());
    }
    
    @Test
    public void shouldSpecial() {
        assertTrue(Pattern.compile(RPattern.toRegx("te\\?*t")).matcher("te?12t").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("te\\?*t")).matcher("te?t").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("te\\?*t")).matcher("te?abct").matches());
        assertTrue(Pattern.compile(RPattern.toRegx("te\\?*")).matcher("te?abct").matches());

        assertFalse(Pattern.compile(RPattern.toRegx("te\\?*")).matcher("1te?abct").matches());
        assertFalse(Pattern.compile(RPattern.toRegx("te\\?*")).matcher("te.abct").matches());
    }
}