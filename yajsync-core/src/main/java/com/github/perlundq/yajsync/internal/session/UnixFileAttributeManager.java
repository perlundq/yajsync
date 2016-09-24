/*
 * Copyright (C) 2016 Per Lundqvist
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
package com.github.perlundq.yajsync.internal.session;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.attr.User;

public final class UnixFileAttributeManager extends FileAttributeManager
{
    private static final Pattern ENTRY_PATTERN = Pattern.compile("^([^:]+):[^:]+:(\\d+):.*$");

    private final Map<Integer, String> _userIdToUserName;
    private final Map<Integer, String> _groupIdToGroupName;
    private final User _defaultUser;
    private final Group _defaultGroup;

    public UnixFileAttributeManager(User defaultUser, Group defaultGroup) throws IOException
    {
        _defaultUser = defaultUser;
        _defaultGroup = defaultGroup;
        _userIdToUserName = readPasswdOrGroup("/etc/passwd");
        _groupIdToGroupName = readPasswdOrGroup("/etc/group");
    }

    private static Map<Integer, String> readPasswdOrGroup(String passwdOrGroup) throws IOException
    {
        Map<Integer, String> idToName = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(passwdOrGroup),
                                                             Charset.defaultCharset())) {
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                Matcher m = ENTRY_PATTERN.matcher(line);
                if (m.matches()) {
                    String name = m.group(1);
                    int id = Integer.parseInt(m.group(2));
                    idToName.put(id, name);
                }
            }
        }
        return idToName;
    }


    @Override
    public RsyncFileAttributes stat(Path path) throws IOException
    {
        Map<String, Object> attrs = Files.readAttributes(path,
                                                         "unix:mode,lastModifiedTime,size,uid,gid",
                                                         LinkOption.NOFOLLOW_LINKS);
        int mode = (int) attrs.get("mode");
        long mtime = ((FileTime) attrs.get("lastModifiedTime")).to(TimeUnit.SECONDS);
        long size = (long) attrs.get("size");
        int uid = (int) attrs.get("uid");
        int gid = (int) attrs.get("gid");

        String userName = _userIdToUserName.getOrDefault(uid, _defaultUser.name());
        String groupName = _groupIdToGroupName.getOrDefault(gid, _defaultGroup.name());
        User user = new User(userName, uid);
        Group group = new Group(groupName, gid);

        return new RsyncFileAttributes(mode, size, mtime, user, group);
    }
}
