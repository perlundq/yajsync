/*
 * @(#) ChecksumDigest.java
 * Created Jun 8, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.internal.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.github.perlundq.yajsync.internal.text.Text;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public interface ChecksumDigest
{
    /**
     * one-shot hash and return digest method.
     * @return digest byte buffer 
     */
    ByteBuffer digest( byte[] buf, int pos, int off, ByteBuffer digest );
    ByteBuffer digest( ByteBuffer buf, ByteBuffer digest );

    void chunk( ByteBuffer buf );
    void chunk( byte[] buf, int pos, int len );

    ByteBuffer digest( ByteBuffer digest );
    
    default ByteBuffer digest() {
        ByteBuffer d = newDigest();
        digest( d ).flip();
        return d;
    }
    
    default ByteBuffer digest( ByteBuffer digest, ByteBuffer... bufs ) {
        int size = Arrays.stream( bufs ).mapToInt( b -> b.limit() ).sum();
        ByteBuffer buf = ByteBuffer.allocate( size );
        for ( ByteBuffer b : bufs ) {
            buf.put( b );
        }
        buf.flip();
        return digest( buf, digest );
    }
    
    ByteBuffer newDigest();
    
    static String toString( ByteBuffer digest )
    {
        return Text.bytesToString( digest );
    }
    
    static boolean match( ByteBuffer digest1, ByteBuffer digest2, int len ) {
        int p1 = digest1.position();
        int p2 = digest2.position();
        for ( int i=0;i<len;i++ ) {
            if ( digest1.get( p1++ ) != digest2.get( p2++ ) )
               return false;
        }
        
        return true;
    }
}
