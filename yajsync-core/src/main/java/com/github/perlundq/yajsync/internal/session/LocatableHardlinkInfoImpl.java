/*
 * @(#) LocatableHardlinkInfoImpl.java
 * Created Jun 17, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.internal.session;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import com.github.perlundq.yajsync.attr.LocatableHardlinkInfo;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class LocatableHardlinkInfoImpl extends HardlinkInfoImpl implements LocatableHardlinkInfo
{
    private final Path _path;

    public LocatableHardlinkInfoImpl( String pathName, ByteBuffer pathNameBytes, RsyncFileAttributes attrs, int linked_idx, Path path )
    {
        super( pathName, pathNameBytes, attrs, linked_idx );
        _path = path;
    }

    public LocatableHardlinkInfoImpl( String pathName, ByteBuffer pathNameBytes, RsyncFileAttributes attrs, int linked_idx, String target, Path path )
    {
        super( pathName, pathNameBytes, attrs, linked_idx, target );
        _path = path;
    }


    @Override
    public Path path()
    {
        return _path;
    }

}
