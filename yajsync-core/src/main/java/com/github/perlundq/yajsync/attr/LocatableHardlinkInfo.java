/*
 * @(#) LocatableHardlinkInfo.java
 * Created Jun 17, 2020 by oleg
 * (C) Odnoklassniki.ru
 */
package com.github.perlundq.yajsync.attr;

import java.nio.file.Path;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public interface LocatableHardlinkInfo extends HardlinkInfo, LocatableFileInfo
{

    Path path();

}
