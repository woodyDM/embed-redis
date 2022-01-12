package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RedisDataType;
import cn.deepmax.redis.core.Sized;
import cn.deepmax.redis.core.support.AbstractRedisObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author wudi
 */
class RString extends AbstractRedisObject implements Sized {
    /**
     * byte is 8-bit
     */
    private byte[] s;

    public RString(TimeProvider timeProvider, byte[] s) {
        super(timeProvider);
        this.s = s;
    }

    public RString(TimeProvider timeProvider) {
        super(timeProvider);
        this.s = new byte[]{};
    }
    
    @Override
    public Type type() {
        return new RedisDataType("string","raw");
    }

    public static RString of(TimeProvider timeProvider, byte[] s, int offset) {
        if (offset == 0) {
            return new RString(timeProvider, s);
        }
        byte[] c = new byte[s.length + offset];
        System.arraycopy(s, 0, c, offset, s.length);
        return new RString(timeProvider, c);
    }

    @Override
    public long size() {
        return s.length;
    }

    /**
     * @param data (elements may be null) treat empty key as bytes 0
     * @return null if key should not be set
     */
    public static RString bitOpAnd(List<RString> data) {
        boolean hasEmptyKey = data.stream().anyMatch(Objects::isNull);
        data = data.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (data.isEmpty()) {
            return null;
        }
        if (!hasEmptyKey && data.size() == 1) {
            return data.get(0).copyTo(null);
        }
        int maxLen = data.stream().mapToInt(d -> d.s.length).max().getAsInt();
        byte[] c = new byte[maxLen];
        if (hasEmptyKey) {
            return new RString(data.get(0).timeProvider, c);
        }
        int minLen = data.stream().mapToInt(d -> d.s.length).min().getAsInt();
        System.arraycopy(data.get(0).s, 0, c, 0, minLen);
        for (int j = 1; j < data.size(); j++) {
            RString it = data.get(j);
            for (int i = 0; i < minLen; i++) {
                c[i] &= it.s[i];
            }
        }
        return new RString(data.get(0).timeProvider, c);
    }

    /**
     * bitop or
     *
     * @param data (elements may be null) treat empty key as bytes 0
     * @return
     */
    public static RString bitOpOr(List<RString> data) {
        data = data.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (data.isEmpty()) {
            return null;
        }
        if (data.size() == 1) {
            return data.get(0).copyTo(null);
        }
        int maxLen = data.stream().mapToInt(d -> d.s.length).max().getAsInt();
        byte[] c = new byte[maxLen];
        System.arraycopy(data.get(0).s, 0, c, 0, data.get(0).s.length);
        for (int j = 1; j < data.size(); j++) {
            RString it = data.get(j);
            for (int i = 0; i < it.s.length; i++) {
                c[i] |= it.s[i];
            }
        }
        return new RString(data.get(0).timeProvider, c);
    }

    /**
     * bitop or
     *
     * @param data (elements may be null) treat empty key as bytes 0
     * @return
     */
    public static RString bitOpXor(List<RString> data) {
        data = data.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (data.isEmpty()) {
            return null;
        }
        if (data.size() == 1) {
            return data.get(0).copyTo(null);
        }
        int maxLen = data.stream().mapToInt(d -> d.s.length).max().getAsInt();
        byte[] c = new byte[maxLen];
        System.arraycopy(data.get(0).s, 0, c, 0, data.get(0).s.length);
        for (int j = 1; j < data.size(); j++) {
            RString it = data.get(j);
            for (int i = 0; i < it.s.length; i++) {
                c[i] ^= it.s[i];
            }
        }
        return new RString(data.get(0).timeProvider, c);
    }

    /**
     * bit op not
     *
     * @param data
     * @return
     */
    public static RString bitOpNot(RString data) {
        if (data == null) {
            return null;
        }
        byte[] c = new byte[data.s.length];
        for (int i = 0; i < data.s.length; i++) {
            c[i] = (byte) ~data.s[i];
        }
        return new RString(data.timeProvider, c);
    }

    public int length() {
        return s.length;
    }

    @Override
    public RString copyTo(Key key) {
        byte[] c = new byte[this.s.length];
        System.arraycopy(this.s, 0, c, 0, this.s.length);
        return new RString(this.timeProvider, c);
    }

    public int setBit(long offset, int v) {
        checkOffset(offset);
        if (v != 0 && v != 1) {
            throw new RedisServerException("ERR bit offset is not an integer or out of range");
        }
        int byteIndex = (int) offset >> 3;  // offset div 8
        if (byteIndex >= s.length) {
            grow(byteIndex + 1);
        }
        int byteval = s[byteIndex];
        int bit = 7 - (int) (offset & 0x7);
        int bitval = byteval & (1 << bit); //old value

        byteval &= ~(1 << bit);         //set pos bit to 0
        byteval |= ((v & 0x1) << bit);  //set real value v at pos
        s[byteIndex] = (byte) byteval;

        return bitval > 0 ? 1 : 0;
    }

    /**
     * 从数组的start到 end 里面的bitCount
     *
     * @param start
     * @param end   include
     * @return
     */
    public long bitCount(int start, int end) {
        start = tranStart(start);
        end = tranEnd(end);
        if (start > end) {
            return 0L;
        }
        long total = 0L;
        for (int i = start; i <= end; i++) {
            byte b = s[i];
            total += _bitCount(b);
        }
        return total;
    }

    public long _bitCount(int b) {
        //can be optimize by table ,but now no need
        int s = 0;
        int t;
        for (int i = 0; i < 8; i++) {
            t = b & (0x1 << i);
            if (t > 0) s++;
        }
        return s;
    }

    //grow to length i
    private void grow(int i) {
        byte[] c = new byte[i];
        System.arraycopy(s, 0, c, 0, s.length);
        this.s = c;
    }

    public int getBit(long offset) {
        checkOffset(offset);
        int byteIndex = (int) offset >> 3;
        if (byteIndex >= s.length) {
            return 0;
        }
        int byteval = s[byteIndex];
        int bit = 7 - (int) (offset & 0x7);
        int bitval = byteval & (1 << bit);
        return bitval > 0 ? 1 : 0;
    }

    private void checkOffset(long offset) {
        if (offset < 0) {
            throw new RedisServerException("ERR bit offset is not an integer or out of range");
        }
    }

    public String str() {
        return new String(s, StandardCharsets.UTF_8);
    }

    public void append(byte[] a) {
        if (a == null || a.length == 0) {
            return;
        }
        byte[] c = new byte[s.length + a.length];
        System.arraycopy(s, 0, c, 0, s.length);
        System.arraycopy(a, 0, c, s.length, a.length);
        setS(c);
    }

    public byte[] getS() {
        return s;
    }

    public void setS(byte[] b) {
        this.s = b;
    }

    public byte[] getRange(int start, int end) {
        start = tranStart(start);
        end = tranEnd(end);
        if (start > end) {
            return new byte[]{};
        }
        byte[] c = new byte[end - start + 1];
        System.arraycopy(s, start, c, 0, c.length);
        return c;
    }

    public void setRange(byte[] value, int offset) {
        int desireLen = value.length + offset;
        if (s.length < desireLen) {
            grow(desireLen);
        }
        System.arraycopy(value, 0, s, offset, value.length);
    }

    public long bitPos(int start, int end, int bit, boolean endGiven) {
        start = tranStart(start);
        end = tranEnd(end);
        if (start > end) {
            return -1L;
        }
        long c = start * 8;
        for (int i = start; i <= end; i++) {
            for (int j = 7; j >= 0; j--) {
                if ((s[i] & (1 << j) ^ (bit << j)) == 0) {
                    return c + (7 - j);
                }
            }
            c += 8;
        }
        if (endGiven) {
            return -1;
        } else if (bit == 1) {
            return -1;
        }
        return c;
    }

    @Override
    public String toString() {
        return "RString["+new String(s)+"]";
    }
}
