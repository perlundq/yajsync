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
package com.github.perlundq.yajsync;

import java.io.IOException;

/**
 * Provides a user name and password when the client authenticates with a server
 * that has requested rsync authentication for an rsync module.
 */
public interface AuthProvider
{
    /**
     * @return the user name to be authenticated as.
     * @throws IOException
     */
    String getUser() throws IOException;

    /**
     * @return password as a char array. The array will be zeroed out as quickly
     *     as possible once it has been used.
     * @throws IOException
     */
    char[] getPassword() throws IOException;
}
