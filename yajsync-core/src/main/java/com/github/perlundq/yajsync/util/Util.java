/*
 * General utility routines
 *
 * Copyright (C) 2013, 2014 Per Lundqvist
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.perlundq.yajsync.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.text.TextDecoder;
import com.github.perlundq.yajsync.text.TextEncoder;

public final class Util
{
    public final static int ERROR_LOG_LEVEL_NUM = 0;
    public final static int WARNING_LOG_LEVEL_NUM = 1;
    public final static int INFO_LOG_LEVEL_NUM = 2;
    public final static int DEBUG1_LOG_LEVEL_NUM = 3;
    public final static int DEBUG2_LOG_LEVEL_NUM = 4;
    public final static int DEBUG3_LOG_LEVEL_NUM = 5;
    public final static int DEBUG4_LOG_LEVEL_NUM = 6;
    private Util() {}

    public static <T> T defaultIfNull(T arg, T defaultValue)
    {
        return arg != null ? arg : defaultValue;
    }

    public static void copyArrays(byte[] src, int srcOffset,
                                  byte[] dst, int dstOffset, int numBytes)
    {
        assert src != null;
        assert srcOffset >= 0;
        assert dst != null;
        assert dstOffset >= 0;
        assert numBytes >= 0;
        assert srcOffset + numBytes <= src.length;
        assert dstOffset + numBytes <= dst.length;
        for (int i = 0; i < numBytes; i++) {
            dst[dstOffset + i] = src[srcOffset + i];
        }
    }

    public static void copyArrays(byte[] src, byte[] dst, int numBytes)
    {
        copyArrays(src, 0, dst, 0, numBytes);
    }

    public static double log2(double n)
    {
        return Math.log(n) / Math.log(2);
    }

    // FIXME: this is so ugly, implement proper base64 encoding with optional padding
    // TODO: update to Java.util.Base64.Encoder from JDK 8 when available
    public static String base64encode(byte[] bytes, boolean pad)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(javax.xml.bind.DatatypeConverter.printBase64Binary(bytes));
        while (!pad && sb.charAt(sb.length() - 1) == '=') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public static void sleep(long millis)
    {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeInterruptException(e);
        }
    }

    public static int randInt(int low, int high)
    {
        int r = Math.abs(new Random().nextInt());
        int range = high - low + 1;
        return (r % range) + low;
    }

    public static boolean randomChance(double percentage)
    {
        assert percentage >= 0 && percentage <= 1;
        return (randInt(0, 100) / 100.0) < percentage;
    }

    public static ByteBuffer slice(ByteBuffer src, int start, int end)
    {
        assert src != null;
        assert start <= end;
        ByteBuffer slice = src.duplicate();
        slice.position(start);
        slice.limit(end);
        return slice;
    }

    /**
     * @throws OverflowException if enlarged buffer would be greater than
     *         maxSize
     */
    public static CharBuffer enlargeCharBuffer(CharBuffer src,
                                               MemoryPolicy policy,
                                               int maxSize)
    {
        int nextSize = src.capacity() * 2;
        if (nextSize <= 0 || nextSize > maxSize) {
            throw new OverflowException(String.format(
                "allocation limit exceeded max is %d", maxSize));
        }

        CharBuffer result = CharBuffer.allocate(nextSize);
        src.flip();
        result.put(src);
        if (policy == MemoryPolicy.ZERO) {
            src.rewind();
            Util.zeroCharBuffer(src);
        }
        return result;
    }

    /**
     * @throws OverflowException if enlarged buffer would be greater than
     *         maxSize
     */
    public static ByteBuffer enlargeByteBuffer(ByteBuffer src,
                                               MemoryPolicy policy,
                                               int maxSize)
    {
        int nextSize = src.capacity() * 2;
        if (nextSize <= 0 || nextSize > maxSize) {
            throw new OverflowException(String.format(
                "allocation limit exceeded max is %d", maxSize));
        }

        ByteBuffer result = ByteBuffer.allocate(nextSize);
        src.flip();
        result.put(src);
        if (policy == MemoryPolicy.ZERO) {
            src.rewind();
            Util.zeroByteBuffer(src);
        }
        return result;
    }

    public static void zeroByteBuffer(ByteBuffer buf)
    {
        Arrays.fill(buf.array(),
                    buf.arrayOffset(),
                    buf.arrayOffset() + buf.limit(),
                    (byte) 0);
    }

    public static void zeroCharBuffer(CharBuffer buf)
    {
        Arrays.fill(buf.array(),
                    buf.arrayOffset(),
                    buf.arrayOffset() + buf.limit(),
                    (char) 0);
    }

    /**
     * NOTE: we don't use Level.CONFIG at all
     */
    public static Level getLogLevelForNumber(int level)
    {
        Level[] logLevels = { Level.SEVERE, Level.WARNING, Level.INFO,
                              Level.FINE, Level.FINER, Level.FINEST };
        return logLevels[Math.min(logLevels.length - 1, level)];
    }

    public static void setRootLogLevel(Level level)
    {
        Logger rootLogger = Logger.getLogger("");
        for (Handler handler : rootLogger.getHandlers()) {
            handler.setLevel(level);
        }
        rootLogger.setLevel(level);
    }

    public static void validateCharset(Charset charset)
    {
        if (!Util.isValidCharset(charset)) {
            throw new UnsupportedCharsetException(String.format(
                    "character set %s is not supported. The charset must be " +
                    "able to encode SLASH (/), DOT (.), NEWLINE (\n), " +
                    "CARRIAGE RETURN (\r) and NULL (\0) to their ASCII " +
                    "counterparts and vice versa", charset));
        }
    }

    /**
     * @returns false if chosen character set cannot encode
     *          SLASH (/), DOT (.), NEWLINE (\n), CARRIAGE RETURN (\r) and
     *          NULL (\0) to their ASCII counterparts and vice versa.
     */
    public static boolean isValidCharset(Charset charset)
    {
        assert charset != null;
        TextEncoder encoder = TextEncoder.newFallback(charset);
        TextDecoder decoder = TextDecoder.newFallback(charset);

        // TODO: add '.' also
        final String testString = Text.SLASH + Text.DOT + '\n' + '\r' + '\0';
        final byte[] expected = { Text.ASCII_SLASH, Text.ASCII_DOT,
                                  Text.ASCII_NEWLINE, Text.ASCII_CR,
                                  Text.ASCII_NULL };

        byte[] encodeResult = encoder.encodeOrNull(testString);
        if (!Arrays.equals(encodeResult, expected)) { // NOTE: returns false if encodeResult is null
            return false;
        }

        String decodeResult =
            decoder.decodeOrNull(ByteBuffer.wrap(encodeResult));
        if (decodeResult == null || !decodeResult.equals(testString)) {
            return false;
        }

        return true;
    }

    public static <T> T firstOf(Iterable<T> list)
    {
        for (T val : list) {
            return val;
        }
        return null;
    }
}
