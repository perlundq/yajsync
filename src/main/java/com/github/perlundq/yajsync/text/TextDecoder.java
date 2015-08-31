/*
 * Character decoding
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
package com.github.perlundq.yajsync.text;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

import com.github.perlundq.yajsync.util.Consts;
import com.github.perlundq.yajsync.util.ErrorPolicy;
import com.github.perlundq.yajsync.util.MemoryPolicy;
import com.github.perlundq.yajsync.util.OverflowException;
import com.github.perlundq.yajsync.util.Util;

public class TextDecoder
{
    private final CharsetDecoder _decoder;

    private TextDecoder(CharsetDecoder decoder)
    {
        _decoder = decoder;
    }

    public static TextDecoder newStrict(Charset charset)
    {
        CharsetDecoder encoder = charset.newDecoder().
            onMalformedInput(CodingErrorAction.REPORT).
            onUnmappableCharacter(CodingErrorAction.REPORT);
        TextDecoder instance = new TextDecoder(encoder);
        return instance;
    }

    public static TextDecoder newFallback(Charset charset)
    {
        CharsetDecoder encoder = charset.newDecoder().
            onMalformedInput(CodingErrorAction.REPLACE).
            onUnmappableCharacter(CodingErrorAction.REPLACE);
        TextDecoder instance = new TextDecoder(encoder);
        return instance;
    }

    public Charset charset()
    {
        return _decoder.charset();
    }

    public String decodeOrNull(ByteBuffer input)
    {
        return _decode(input, ErrorPolicy.RETURN_NULL, MemoryPolicy.IGNORE);
    }

    /**
     *  @throws TextConversionException
     */
    public String decode(ByteBuffer input)
    {
        return _decode(input, ErrorPolicy.THROW, MemoryPolicy.IGNORE);
    }

    /**
     *  @throws TextConversionException
     */
    public String secureDecode(ByteBuffer input)
    {
        return _decode(input, ErrorPolicy.THROW, MemoryPolicy.ZERO);
    }

    /**
     *  @throws TextConversionException
     */
    private String _decode(ByteBuffer input,
                           ErrorPolicy errorPolicy,
                           MemoryPolicy memoryPolicy)
    {
        _decoder.reset();
        CharBuffer output = CharBuffer.allocate(
                                (int) Math.ceil(input.capacity() *
                                                _decoder.averageCharsPerByte()));
        try {
            CoderResult result;
            while (true) {
                result = _decoder.decode(input, output, true);
                if (result.isOverflow()) {
                    output = Util.enlargeCharBuffer(output, memoryPolicy,
                                                    Consts.MAX_BUF_SIZE / 2);   // throws OverflowException
                } else {
                    break;
                }
            }

            while (!result.isError()) {
                result = _decoder.flush(output);
                if (result.isOverflow()) {
                    output = Util.enlargeCharBuffer(output, memoryPolicy,
                                                    Consts.MAX_BUF_SIZE / 2);   // throws OverflowException
                } else {
                    break;
                }
            }

            if (result.isUnderflow()) {
                return output.flip().toString();
            }

            if (errorPolicy == ErrorPolicy.THROW) {
                input.limit(input.position() + result.length());
                throw new TextConversionException(String.format(
                    "%s failed to decode %d bytes after %s (using %s): ",
                    result, result.length(), output.flip().toString(),
                    _decoder.charset(),
                    Text.byteBufferToString(input)));
            }
            return null;
        } catch (OverflowException e) {
            if (errorPolicy == ErrorPolicy.THROW) {
                throw new TextConversionException(e);
            }
            return null;
        } finally {
            if (memoryPolicy == MemoryPolicy.ZERO) {
                Util.zeroCharBuffer(output);
            }
        }
    }

    public String decodeOrNull(byte[] bytes)
    {
        ByteBuffer input = ByteBuffer.wrap(bytes);
        return decodeOrNull(input);
    }
}
