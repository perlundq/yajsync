/*
 * @(#) MD5Digest.java
 * Created Jun 9, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.internal.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class MD5Digest implements ChecksumDigest
{
    private static final String MD5_NAME = "MD5";

    private final long seed;
    private final byte[] seedbytes;

    private MessageDigest md;

    public MD5Digest()
    {
        this.seed = 0;
        this.seedbytes = new byte[0];
        this.md = newInstance();
    }
    
    @Override
    public ByteBuffer newDigest()
    {
        return ByteBuffer.allocate( 16 );
    }

    public MD5Digest( int seed )
    {
        this.seed = seed;
        this.seedbytes = seed == 0 ? new byte[0] : BitOps.toLittleEndianBuf( seed );
        this.md = newInstance();
    }

    private static MessageDigest newInstance()
    {
        try {
            return MessageDigest.getInstance(MD5_NAME);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);              // support for MD5 is required so this should not happen
        }
    }
    
    @Override
    public ByteBuffer digest( byte[] buf, int pos, int len, ByteBuffer digest )
    {
        md.update( buf, pos, len );
        digest.put( md.digest( seedbytes ) );
        return digest;
    }

    @Override
    public ByteBuffer digest( ByteBuffer buf, ByteBuffer digest )
    {
        md.update( buf );
        digest.put( md.digest( seedbytes ) );
        return digest;
    }
    
    @Override
    public void chunk( byte[] buf, int pos, int len )
    {
        assert seed == 0;
        
        if ( len == 0 )
            return;

        assert md != null;
        md.update( buf, pos, len );
    }

    @Override
    public void chunk( ByteBuffer buf )
    {
        assert seed == 0;

        if ( buf.remaining() == 0)
            return;
        
        assert md != null;
        md.update( buf );
    }
    
    @Override
    public ByteBuffer digest( ByteBuffer digest )
    {
        digest.put( md.digest() );
        return digest;
    }

}
