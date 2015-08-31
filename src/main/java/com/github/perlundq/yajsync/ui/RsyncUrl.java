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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.PathOps;

final class RsyncUrl
{
    /*
      [USER@]HOST::SRC...
      rsync://[USER@]HOST[:PORT]/SRC
    */
    private static final String USER_REGEX = "[^@: ]+@";
    private static final String HOST_REGEX = "[^:/]+";
    private static final String PORT_REGEX = ":[0-9]+";
    private static final String MODULE_REGEX = "[^/]+";
    private static final String PATH_REGEX = "/.*";
    private static final Pattern MODULE =
            Pattern.compile(String.format("^(%s)?(%s)::(%s)?(%s)?$",
                                          USER_REGEX, HOST_REGEX,
                                          MODULE_REGEX, PATH_REGEX));
    private static final Pattern URL =
            Pattern.compile(String.format("^rsync://(%s)?(%s)(%s)?(/%s)?(%s)?$",
                                          USER_REGEX, HOST_REGEX, PORT_REGEX,
                                          MODULE_REGEX, PATH_REGEX));

    private final ConnInfo _connInfo;
    private final String _moduleName;
    private final String _pathName;

    public RsyncUrl(ConnInfo connInfo, String moduleName, String pathName)
            throws IllegalUrlException
    {
        assert pathName != null;
        assert moduleName != null;
        assert connInfo != null || moduleName.isEmpty() : connInfo + " " + moduleName;
        if (connInfo != null && moduleName.isEmpty() && !pathName.isEmpty()) {
            throw new IllegalUrlException(String.format(
                    "remote path %s specified without a module", pathName));
        }
        _connInfo = connInfo;
        _moduleName = moduleName;
        if (connInfo == null) {
            _pathName = toLocalPathName(pathName);
        } else {
            _pathName = toRemotePathName(_moduleName, pathName);
        }
    }

    private static RsyncUrl local(String pathName)
    {
        try {
            return new RsyncUrl(null, "", pathName);
        } catch (IllegalUrlException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isRemote()
    {
        return _connInfo != null;
    }

    public boolean isLocal()
    {
        return !isRemote();
    }

    public ConnInfo connInfoOrNull()
    {
        return _connInfo;
    }

    public String moduleName()
    {
        return _moduleName;
    }

    public String pathName()
    {
        return _pathName;
    }

    @Override
    public String toString()
    {
        if (_connInfo != null) {
            return String.format("%s/%s/%s", _connInfo, _moduleName, _pathName);
        }
        return _pathName;
    }

    private static RsyncUrl matchModule(String arg) throws IllegalUrlException
    {
        Matcher mod = MODULE.matcher(arg);
        if (!mod.matches()) {
            return null;
        }
        String userName = Text.stripLast(Text.nullToEmptyStr(mod.group(1)));
        String address = mod.group(2);
        String moduleName = Text.nullToEmptyStr(mod.group(3));
        String pathName = Text.nullToEmptyStr(mod.group(4));
        ConnInfo connInfo = new ConnInfo.Builder(address).
                userName(userName).build();
        return new RsyncUrl(connInfo, moduleName, pathName);
    }

    private static RsyncUrl matchUrl(String arg) throws IllegalUrlException
    {
        Matcher url = URL.matcher(arg);
        if (!url.matches()) {
            return null;
        }
        String userName = Text.stripLast(Text.nullToEmptyStr(url.group(1)));
        String address = url.group(2);
        String moduleName = Text.stripFirst(Text.nullToEmptyStr(url.group(4)));
        ConnInfo.Builder connInfoBuilder =
                new ConnInfo.Builder(address).userName(userName);
        if (url.group(3) != null) {
            int portNumber = Integer.parseInt(Text.stripFirst(url.group(3)));
            connInfoBuilder.portNumber(portNumber);
        }
        String pathName = Text.nullToEmptyStr(url.group(5));
        return new RsyncUrl(connInfoBuilder.build(), moduleName, pathName);
    }

    public static RsyncUrl parse(String arg) throws IllegalUrlException
    {
        assert arg != null;
        if (arg.isEmpty()) {
            throw new IllegalUrlException("empty string");
        }
        RsyncUrl result = matchModule(arg);
        if (result != null) {
            return result;
        }
        result = matchUrl(arg);
        if (result != null) {
            return result;
        }
        return RsyncUrl.local(arg);
    }

    public static String toLocalPathName(String pathName)
    {
        assert !pathName.isEmpty();
        Path p = PathOps.get(pathName);
        if (p.isAbsolute()) {
            return p.toString();
        }
        return Environment.getWorkingDirectory().resolve(p).toString();
    }

    private static String toRemotePathName(String moduleName, String pathName)
    {
        if (moduleName.isEmpty() && pathName.isEmpty()) {
            return "";
        }
        if (pathName.isEmpty()) {
            return moduleName + Text.SLASH;
        }
        return moduleName + pathName;
    }
}
