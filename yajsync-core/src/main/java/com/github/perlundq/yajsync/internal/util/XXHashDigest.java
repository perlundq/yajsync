/*
 * @(#) XXHashDigest.java
 * Created Jun 8, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.internal.util;

import java.nio.ByteBuffer;

import net.openhft.hashing.LongHashFunction;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class XXHashDigest implements ChecksumDigest
{
    private LongHashFunction xxhash ;
    
    public XXHashDigest( long seed )
    {
        this.xxhash = LongHashFunction.xx( seed );
    }
    
    @Override
    public ByteBuffer newDigest()
    {
        return ByteBuffer.allocate( Consts.SIZE_LONG );
    }

    @Override
    public ByteBuffer digest( byte[] buf, int pos, int len, ByteBuffer digest )
    {
        return digest.putLong( this.xxhash.hashBytes( buf, pos, len ) );
    }

    @Override
    public ByteBuffer digest( ByteBuffer buf, ByteBuffer digest )
    {
        return digest.putLong( this.xxhash.hashBytes( buf ) );
    }

    @Override
    public void chunk( byte[] buf, int pos, int len )
    {
        if ( len == 0 )
            return;

        this.xxhash = LongHashFunction.xx( this.xxhash.hashBytes( buf, pos, len ) );
    }
    
    @Override
    public void chunk( ByteBuffer buf )
    {
        if ( buf.remaining() == 0 )
            return;

        this.xxhash = LongHashFunction.xx( this.xxhash.hashBytes( buf ) );
    }
    
    @Override
    public ByteBuffer digest( ByteBuffer digest )
    {
        return digest.putLong( this.xxhash.hashVoid() );
    }
}
