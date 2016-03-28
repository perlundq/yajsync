/*
 * Copyright (C) 2013-2015 Per Lundqvist
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
package com.github.perlundq.yajsync.ui;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

final class RsyncUrls
{
    private final ConnInfo _connInfo;
    private final String _moduleName;
    private final Iterable<String> _pathNames;

    public RsyncUrls(ConnInfo connInfo, String moduleName,
                     Iterable<String> pathNames)
    {
        _connInfo = connInfo;
        _moduleName = moduleName;
        _pathNames = pathNames;
    }

    public RsyncUrls(Path cwd, Iterable<String> urls) throws IllegalUrlException
    {
        List<String> pathNames = new LinkedList<>();
        RsyncUrl prevUrl = null;
        String moduleName = null;
        for (String s : urls) {
            RsyncUrl url = RsyncUrl.parse(cwd, s);
            boolean isFirst = prevUrl == null;
            boolean curAndPrevAreLocal = !isFirst &&
                                         url.isLocal() && prevUrl.isLocal();
            boolean curAndPrevIsSameRemote = !isFirst &&
                    url.isRemote() && prevUrl.isRemote() &&
                    url.connInfoOrNull().equals(prevUrl.connInfoOrNull()) &&
                    url.moduleName().equals(prevUrl.moduleName());
            if (isFirst || curAndPrevAreLocal || curAndPrevIsSameRemote) {
                if (moduleName == null && url.isRemote()) {
                    moduleName = url.moduleName();
                }
                if (!url.pathName().isEmpty()) {
                    pathNames.add(url.pathName());
                }
                prevUrl = url;
            } else {
                throw new IllegalUrlException(String.format(
                        "remote source arguments %s and %s are incompatible",
                        prevUrl, url));
            }
        }
        if (prevUrl == null) {
            throw new IllegalArgumentException("empty sequence: " + urls);
        }
        _pathNames = pathNames;
        _moduleName = moduleName;
        _connInfo = prevUrl.connInfoOrNull();
    }

    public String moduleName()
    {
        return _moduleName;
    }

    @Override
    public String toString()
    {
        if (isRemote()) {
            return String.format("%s/%s%s",
                                 _connInfo, _moduleName, _pathNames.toString());
        }
        return _pathNames.toString();
    }

    public boolean isRemote()
    {
        return _connInfo != null;
    }

    public ConnInfo connInfoOrNull()
    {
        return _connInfo;
    }

    public Iterable<String> pathNames()
    {
        return _pathNames;
    }
}
