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
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

import com.github.perlundq.yajsync.util.Environment;

public final class User extends AbstractPrincipal
{
    private static final User ROOT = new User("root", 0);
    private static final User NOBODY = new User("nobody", ID_NOBODY);
    private static final User JVM_USER = new User(Environment.getUserName(),
                                                  Environment.getUserId());

    public User(String name, int uid)
    {
        super(name, uid);
    }

    public static User whoami()
    {
        return JVM_USER;
    }

    public static User root()
    {
        return ROOT;
    }

    public static User nobody()
    {
        return NOBODY;
    }

    public UserPrincipal userPrincipal() throws IOException
    {
        UserPrincipalLookupService lookupService =
            FileSystems.getDefault().getUserPrincipalLookupService();
        return lookupService.lookupPrincipalByName(_name);
    }
}
