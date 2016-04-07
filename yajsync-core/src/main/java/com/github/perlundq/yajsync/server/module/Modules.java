/*
 * A handle for all the modules available to a given Principal.
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

public interface Modules
{
    /**
     * @return a Module directly available to the principal or a
     *         RestrictedModule in case further authentication is required
     *         (using rsync challenge response protocol).
     * @throws ModuleSecurityException if principal should be denied access to
     *         this module.
     * @throws ModuleException for any other errors that is expected to be
     *         handled.
     */
    Module get(String moduleName) throws ModuleException;

    /**
     * @return an iterable over all modules available to the principal connected
     *         to this instance. Any module should be an instance of
     *         RestrictedModule if further authentication is required.
     */
    Iterable<Module> all();
}
