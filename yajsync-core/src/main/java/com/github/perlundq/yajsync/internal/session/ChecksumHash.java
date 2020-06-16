/*
 * @(#) SessionHash.java
 * Created Jun 8, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.internal.session;

import com.github.perlundq.yajsync.internal.util.ChecksumDigest;
import com.github.perlundq.yajsync.internal.util.MD5Digest;
import com.github.perlundq.yajsync.internal.util.XXHashDigest;

/**
 * A hash to use for files integrity control.
 * The default, MD5 is default and slow, provided only for compatibility with rsync protocol.
 * 
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public enum ChecksumHash
{
    md5 {
        @Override
        ChecksumDigest instance( int seed )
        {
            return new MD5Digest( seed );
        }
        @Override
        int maxlength()
        {
            return 16;
        }
    },
    xxhash {
        @Override
        ChecksumDigest instance( int seed )
        {
            return new XXHashDigest( seed );
        }
        
        @Override
        int maxlength()
        {
            return 8;
        }
    };

    abstract ChecksumDigest instance( int seed );
    abstract int maxlength();
}
