/*
 * Copyright (C) 2014 Per Lundqvist
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
package com.github.perlundq.yajsync.session;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.github.perlundq.yajsync.text.Text;

@RunWith(Parameterized.class)
public class IntegerCoderTest
{
    private static final int _minBytes = 3;
    private long _input;
    private ByteBuffer _expected;

    public IntegerCoderTest(long input, byte[] expected)
    {
        _input = input;
        _expected = ByteBuffer.wrap(expected);
    }

    @Parameters
    public static Collection<Object[]> testsCollection()
    {
        long val_1  = Long.MIN_VALUE;
        byte res_1[] = { -4, 0, 0, 0, 0, 0, 0, 0, -128 };

        long val_2 = -4611686018427387905L; // -(2**62) - 1
        byte res_2[] = { -4, -1, -1, -1, -1, -1, -1, -1, -65 };

        long val_3 = val_2 + 1;
        byte res_3[] = { -4, 0, 0, 0, 0, 0, 0, 0, -64 };

        long val_4 = val_2 + 2;
        byte res_4[] = { -4, 1, 0, 0, 0, 0, 0, 0, -64 };

        long val_5 = -2;
        byte res_5[] = { -4, -2, -1, -1, -1, -1, -1, -1, -1 };

        long val_6 = -1;
        byte res_6[] = { -4, -1, -1, -1, -1, -1, -1, -1, -1 };

        long val_7 = 0;
        byte res_7[] = { 0, 0, 0 };

        long val_8 = 1;
        byte res_8[] = { 0, 1, 0 };

        long val_9 = 255;
        byte res_9[] = { 0, -1, 0 };

        long val_10 = 256;
        byte res_10[] = { 0, 0, 1 };

        long val_11 = 257;
        byte res_11[] = { 0, 1, 1 };

        long val_12 = 511;
        byte res_12[] = { 0, -1, 1 };

        long val_13 = 512;
        byte res_13[] = { 0, 0, 2 };

        long val_14 = 524287; // 2**19 - 1
        byte res_14[] = { 7, -1, -1 };

        long val_15 = val_14 + 1;
        byte res_15[] = { 8, 0, 0};

        long val_16 = 4294967295L; // 2**32 - 1
        byte res_16[] = { -64, -1, -1, -1, -1 };

        long val_17 = 4294967296L;
        byte res_17[] = { -63, 0, 0, 0, 0 };

        long val_18 = Long.MAX_VALUE - 1;
        byte res_18[] = { -4, -2, -1, -1, -1, -1, -1, -1, 127 };

        long val_19 = Long.MAX_VALUE;
        byte res_19[] = { -4, -1, -1, -1, -1, -1, -1, -1, 127 };

        return Arrays.asList(new Object[][] {
            { val_1, res_1 }, { val_2, res_2 }, { val_3, res_3 },
            { val_4, res_4 }, { val_5, res_5 }, { val_6, res_6 },
            { val_7, res_7 }, { val_8, res_8 }, { val_9, res_9 },
            { val_10, res_10 }, { val_11, res_11 }, { val_12, res_12 },
            { val_13, res_13 }, { val_14, res_14 }, { val_15, res_15 },
            { val_16, res_16 }, { val_17, res_17 }, { val_18, res_18 },
            { val_19, res_19 } });
    }

    @Test
    public void testEncodingCorrectness()
    {
        ByteBuffer actual = IntegerCoder.encodeLong(_input, _minBytes);
        assertEquals(String.format("encode %d -> %s, expected %s (minBytes=%d)",
                                   _input,
                                   Text.byteBufferToString(actual),
                                   Text.byteBufferToString(_expected),
                                   _minBytes),
                     _expected, actual);
    }

    @Test
    public void testEncodingDecodingSymmetry() throws Exception
    {
        ByteBuffer encodedBytes = IntegerCoder.encodeLong(_input, _minBytes);
        ReadableByteBuffer rbb = new ReadableByteBuffer(encodedBytes);
        long decoded = IntegerCoder.decodeLong(rbb, _minBytes);
        assertEquals(_input, decoded);
    }
}
