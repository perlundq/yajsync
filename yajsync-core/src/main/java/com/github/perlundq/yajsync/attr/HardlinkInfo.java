/*
 * @(#) HardlinkInfo.java
 * Created Jun 17, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.attr;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public interface HardlinkInfo extends FileInfo
{

    boolean isFirst();

    int getLinkedIndex();

    void setLinkedIndex( int linked );

    String targetPathName();
    void setTargetPathName( String path );

    boolean isAbbrev();
    
    void setAbbrev( boolean abbr );

}
