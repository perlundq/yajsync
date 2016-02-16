/*
 * Copyright (C) 2014-2016 Per Lundqvist
 * Copyright (C) 2015-2016 Florian Sager
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
package com.github.perlundq.yajsync.filelist;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

import com.github.perlundq.yajsync.util.Environment;

public final class Group extends AbstractPrincipal
{
    private static final Group ROOT = new Group("root", 0);
    private static final Group NOBODY = new Group("nobody", ID_NOBODY);
    private static final Group JVM_USER = new Group(Environment.getGroupName(),
                                                    Environment.getGroupId());

    public Group(String name, int gid)
    {
        super(name, gid);
    }

    public static Group whoami()
    {
        return JVM_USER;
    }

    public static Group root()
    {
        return ROOT;
    }

    public static Group nobody()
    {
        return NOBODY;
    }

    public GroupPrincipal groupPrincipal() throws IOException
    {
        UserPrincipalLookupService lookupService =
            FileSystems.getDefault().getUserPrincipalLookupService();
        return lookupService.lookupPrincipalByGroupName(_name);
    }
}
