/*
 * A restricted Module requiring rsync MD5 challenge response authentication to
 * a regular Module
 *
 * Copyright (C) 2014 Per Lundqvist
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
package com.github.perlundq.yajsync.server.module;

/**
 * A RestrictedModule is a Module that only provides access to its name and
 * comment attributes. All Module method invocations other than name() or
 * comment() will throw an UnsupportedOperationException.
 *
 * authenticate() is invoked by ServerSessionConfig to determine the correct
 * response when using rsync MD5 challenge response protocol. If the response
 * from authenticate matches what peer has supplied, toModule is invoked to get
 * a regular, unrestricted Module.
 */
public abstract class RestrictedModule implements Module
{
    /**
     * Returns the expected response for user given the supplied authContext.
     * If the response matches the user supplied response this will be followed
     * by a call to toModule.
     *
     * @param authContext the authContext used for computing the response.
     * @param userName the userName to use with authContext.response
     * @return the expected response for userName computed from
     *         authContext.response()
     */
    abstract public String authenticate(RsyncAuthContext authContext,
                                        String userName)
        throws ModuleSecurityException;

    /**
     * @return the corresponding regular, unrestricted Module of this instance.
     */
    abstract public Module toModule();

    @Override
    abstract public String name();

    @Override
    abstract public String comment();

    @Override
    public final RestrictedPath restrictedPath()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isReadable()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean isWritable()
    {
        throw new UnsupportedOperationException();
    }
}
