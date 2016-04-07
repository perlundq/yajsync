/*
 * Plugin-based module provider
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

import java.net.InetAddress;
import java.security.Principal;
import java.util.Collection;
import java.util.ServiceLoader;

import com.github.perlundq.yajsync.internal.util.Option;

public abstract class ModuleProvider
{
    public static ModuleProvider getDefault()
    {
        ServiceLoader<ModuleProvider> loader =
            ServiceLoader.load(ModuleProvider.class);
        for (ModuleProvider provider : loader) {
            return provider;
        }
        return new Configuration.Reader();
    }

    public abstract Collection<Option> options();
    public abstract void close();

    // must be thread safe
    public abstract Modules newAuthenticated(InetAddress address,
                                             Principal principal)
        throws ModuleException;

    // must be thread safe
    public abstract Modules newAnonymous(InetAddress address)
        throws ModuleException;

}
