/*
 * rsync challenge response MD5 authentication
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.perlundq.yajsync.server.module;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.text.TextEncoder;
import com.github.perlundq.yajsync.internal.util.Consts;
import com.github.perlundq.yajsync.internal.util.MD5Digest;
import com.github.perlundq.yajsync.internal.util.Util;

public class RsyncAuthContext
{
    private final String _challenge;
    private final TextEncoder _characterEncoder;

    private RsyncAuthContext(TextEncoder characterEncoder, String challenge)
    {
        _challenge = challenge;
        _characterEncoder = characterEncoder;
    }

    public RsyncAuthContext(TextEncoder characterEncoder)
    {
        _challenge = newChallenge();
        _characterEncoder = characterEncoder;
    }

    public static RsyncAuthContext fromChallenge(TextEncoder characterEncoder,
                                                 String challenge)
    {
        return new RsyncAuthContext(characterEncoder, challenge);
    }

    public String challenge()
    {
        return _challenge;
    }

    /**
     * @throws TextConversionException if _challenge cannot be encoded
     */
    public String response(char[] password)
    {
        ByteBuffer encoded = Base64.getEncoder().withoutPadding().encode( hash( password ) );
        return new String( encoded.array(), encoded.arrayOffset(), encoded.limit());
    }

    private String newChallenge()
    {
        long rand = new SecureRandom().nextLong();
        byte[] randBytes = ByteBuffer.allocate(Consts.SIZE_LONG).putLong(rand).array();
        return Base64.getEncoder().withoutPadding().encodeToString(randBytes);
    }

    /**
     * @throws TextConversionException if _challenge cannot be encoded
     */
    private ByteBuffer hash(char[] password)
    {
        ByteBuffer passwordBytes = null;
        try {
            passwordBytes = _characterEncoder.secureEncodeOrNull(password);
            if (passwordBytes == null) {
                throw new RuntimeException("Unable to encode characters in " +
                                           "password");
            }
            ByteBuffer challengeBytes = _characterEncoder.encode(_challenge);       // throws TextConversionException
            
            MD5Digest md5 = new MD5Digest();
            return (ByteBuffer) md5.digest( md5.newDigest(), passwordBytes, challengeBytes ).flip();
        } finally {
            if (passwordBytes != null) {
                Util.zeroByteBuffer( passwordBytes );
            }
        }
    }
}
