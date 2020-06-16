/*
 * Character encoding
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
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;

import com.github.perlundq.yajsync.internal.util.Consts;
import com.github.perlundq.yajsync.internal.util.ErrorPolicy;
import com.github.perlundq.yajsync.internal.util.MemoryPolicy;
import com.github.perlundq.yajsync.internal.util.OverflowException;
import com.github.perlundq.yajsync.internal.util.Util;

public class TextEncoder
{
    private final CharsetEncoder _encoder;

    private TextEncoder(CharsetEncoder encoder)
    {
        _encoder = encoder;
    }

    public static TextEncoder newStrict(Charset charset)
    {
        CharsetEncoder encoder = charset.newEncoder().
            onMalformedInput(CodingErrorAction.REPORT).
            onUnmappableCharacter(CodingErrorAction.REPORT);
        TextEncoder instance = new TextEncoder(encoder);
        return instance;
    }

    public static TextEncoder newFallback(Charset charset)
    {
        CharsetEncoder encoder = charset.newEncoder().
            onMalformedInput(CodingErrorAction.REPLACE).
            onUnmappableCharacter(CodingErrorAction.REPLACE);
        TextEncoder instance = new TextEncoder(encoder);
        return instance;
    }

    public Charset charset()
    {
        return _encoder.charset();
    }

    /**
     * @throws TextConversionException 
     */
    private ByteBuffer encode(CharBuffer input,
                          ErrorPolicy errorPolicy,
                          MemoryPolicy memoryPolicy)
    {
        _encoder.reset();
        ByteBuffer output = ByteBuffer.allocate(
                                (int) Math.ceil(input.capacity() *
                                                _encoder.averageBytesPerChar()));
        try {
            CoderResult result;
            while (true) {
                result = _encoder.encode(input, output, true);
                if (result.isOverflow()) {
                    output = Util.enlargeByteBuffer(output, memoryPolicy,
                                                    Consts.MAX_BUF_SIZE);
                } else {
                    break;
                }
            }

            while (!result.isError()) {
                result = _encoder.flush(output);
                if (result.isOverflow()) {
                    output = Util.enlargeByteBuffer(output, memoryPolicy,
                                                    Consts.MAX_BUF_SIZE);
                } else {
                    break;
                }
            }

            if (result.isUnderflow()) {
                output.flip();
                return output;
            }

            if (errorPolicy == ErrorPolicy.THROW) { // NOTE: in some circumstances we should avoid printing the contents
                input.limit(input.position() + result.length());
                throw new TextConversionException(String.format(
                    "failed to encode %d bytes after %s (using %s): %s -> %s",
                    result.length(), output.flip().toString(),
                    _encoder.charset(), Text.charBufferToString(input),
                    result));
            }
            return null;
        } catch (OverflowException e) {
            if (errorPolicy == ErrorPolicy.THROW) {
                throw new TextConversionException(e);
            }
            return null;
        } finally {
            if (memoryPolicy == MemoryPolicy.ZERO) {
                Util.zeroByteBuffer(output);
            }
        }
    }

    public ByteBuffer secureEncodeOrNull(char[] inputChars)
    {
        CharBuffer input = CharBuffer.wrap(inputChars);
        return encode(input,
                      ErrorPolicy.RETURN_NULL,
                      MemoryPolicy.ZERO);
    }

    public ByteBuffer encodeOrNull(String string)
    {
        char[] inputChars = string.toCharArray();
        CharBuffer input = CharBuffer.wrap(inputChars);
        return encode(input,
                      ErrorPolicy.RETURN_NULL,
                      MemoryPolicy.IGNORE);
    }
    
    /**
     * @throws TextConversionException 
     */
    public ByteBuffer encode(String string)
    {
        char[] inputChars = string.toCharArray();
        CharBuffer input = CharBuffer.wrap(inputChars);
        return encode(input, ErrorPolicy.THROW, MemoryPolicy.IGNORE);
    }
}
