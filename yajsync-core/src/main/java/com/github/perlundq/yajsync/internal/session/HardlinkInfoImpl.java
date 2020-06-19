/*
 * @(#) HardlinkInfoImpl.java
 * Created Jun 17, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.internal.session;

import java.nio.ByteBuffer;

import com.github.perlundq.yajsync.attr.HardlinkInfo;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
class HardlinkInfoImpl extends FileInfoImpl implements HardlinkInfo
{
    private int _linked_idx;
    private String _targetPathName;
    private boolean _abbr;

    public HardlinkInfoImpl(String pathName, ByteBuffer pathNameBytes,
                    RsyncFileAttributes attrs, int linked_idx)
    {
        this( pathName, pathNameBytes, attrs, linked_idx, null );
    }

    public HardlinkInfoImpl(String pathName, ByteBuffer pathNameBytes,
                    RsyncFileAttributes attrs, int linked_idx, String target)
    {
        super( pathName, pathNameBytes, attrs );
        _linked_idx = linked_idx;
        _targetPathName = target;
    }

    @Override
    public boolean isFirst() {
        return _linked_idx < 0;
    }
    
    @Override
    public boolean isAbbrev() {
        return _abbr;
    }
    
    @Override
    public void setAbbrev( boolean abbr )
    {
        _abbr = abbr;
    }

    @Override
    public int getLinkedIndex() {
        assert _linked_idx >= 0;
        return _linked_idx;
    }
    
    @Override
    public void setLinkedIndex( int linked )
    {
        assert _linked_idx < 0;
        _linked_idx = linked;
    }
    
    @Override
    public String targetPathName()
    {
        return _targetPathName;
    }

    @Override
    public void setTargetPathName(String path) {
        assert path != null;
        _targetPathName = path;
    }
    
}
