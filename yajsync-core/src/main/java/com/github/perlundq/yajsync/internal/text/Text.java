/*
 * Text utility routines
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
package com.github.perlundq.yajsync.internal.text;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.internal.util.Util;

public final class Text
{
    public static final String BACK_SLASH = "\\";
    public static final String DOT = ".";
    public static final String DOT_DOT = "..";
    public static final String EMPTY = "";
    public static final String SLASH = "/";
    public static final String ASCII_NAME = "US-ASCII";
    public static final String UTF8_NAME = "UTF-8";
    public static final byte ASCII_NULL = 0x00;     // ASCII '\0'
    public static final byte ASCII_NEWLINE = 0x0A;  // ASCII '\n'
    public static final byte ASCII_CR = 0x0D;       // ASCII '\r'
    public static final byte ASCII_DOT = 0x2E;      // ASCII '.'
    public static final byte ASCII_SLASH = 0x2F;    // ASCII '/'

    private Text() {}

    public static String stripLast(String str)
    {
        assert str != null;
        return str.isEmpty() ? "" : str.substring(0, str.length() - 1);
    }

    public static String stripFirst(String str)
    {
        assert str != null;
        return str.isEmpty() ? "" :  str.substring(1);
    }

    public static String nullToEmptyStr(String arg)
    {
        return Util.defaultIfNull(arg, "");
    }

    public static String join(Iterable<String> strings, String separator)
    {
        StringBuilder sb = new StringBuilder();
        for (Iterator<String> it = strings.iterator(); it.hasNext(); ) {
            String s = it.next();
            sb.append(s);
            if (it.hasNext()) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }

    public static String withSlashAsPathSepator(Path path)
    {
        assert !path.isAbsolute();
        String separator = path.getFileSystem().getSeparator();
        if (separator.equals(Text.SLASH)) {
            return path.toString();
        }
        String regex = Pattern.quote(separator);
        String pathName = path.toString();
        String[] split = pathName.split(regex);
        return join(Arrays.asList(split), Text.SLASH);
    }

    public static String deleteTrailingDots(String entry)
    {
        return deleteTrailingChars(entry, '.');
    }

    private static String deleteTrailingChars(String entry, char c)
    {
        StringBuilder sb = new StringBuilder(entry.toString());
        int index = sb.length();
        while (index > 0 && sb.charAt(index - 1) == c) {
            index--;
        }
        sb.delete(index, sb.length());
        return sb.toString();
    }

    public static String bytesToString(byte[] buf)
    {
        return bytesToString(ByteBuffer.wrap(buf));
    }

    public static String bytesToString(ByteBuffer buf)
    {
        StringBuilder sb = new StringBuilder();
        ByteBuffer dup = buf.duplicate();
        sb.append("[");
        while (dup.hasRemaining()) {
            sb.append(String.format("0x%02x", dup.get()));
            if (dup.hasRemaining()) {
                sb.append(", ");
            }
        }
        return sb.append("]").toString();
    }

    public static String charBufferToString(CharBuffer buf)
    {
        StringBuilder sb = new StringBuilder();
        CharBuffer dup = buf.duplicate();
        sb.append("[");
        while (dup.hasRemaining()) {
            sb.append(dup.get());
            if (dup.hasRemaining()) {
                sb.append(", ");
            }
        }
        return sb.append("]").toString();
    }

    public static String byteBufferToString(ByteBuffer buf, int start, int end)
    {
        return bytesToString(Util.slice(buf, start, end));
    }

    public static String charBufferToString(CharBuffer buf, int start, int end)
    {
        CharBuffer dup = buf.duplicate();
        dup.position(start);
        dup.limit(end);
        return charBufferToString(dup);
    }
}
