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
package com.github.perlundq.yajsync.util;

public final class Triple<S, T, U>
{
    private final S _first;
    private final T _second;
    private final U _third;

    public Triple(S first, T second, U third)
    {
        _first = first;
        _second = second;
        _third = third;
    }

    public S first()
    {
        return _first;
    }

    public T second()
    {
        return _second;
    }

    public U third()
    {
        return _third;
    }
}
