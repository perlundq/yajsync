/*
 * MD5 utility routines
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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class MD5
{
    private static final String MD5_NAME = "MD5";

    private MD5() {}

    public static MessageDigest newInstance()
    {
        try {
            return MessageDigest.getInstance(MD5_NAME);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);              // support for MD5 is required so this should not happen
        }
    }

    public static String md5DigestToString(byte[] digestBuf)
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < digestBuf.length; i++) {
            sb.append(Integer.toHexString(0xFF & digestBuf[i]));
        }
        return sb.toString();
    }
}
