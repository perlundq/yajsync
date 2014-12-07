/*
 * A barebone partial implementation of a multimap
 *
 * Copyright (C) 2013, 2014 Per Lundqvist
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Multimap<K, V>
{
    private final Map<K,List<V>> _map;
    private int _size;
    
    public Multimap(int size)
    {
        _map = new HashMap<>((int) Math.ceil(size / 0.75));
    }

    // we trust the caller to not modify the returned collection
    public List<V> get(K key)
    {
        List<V> values = _map.get(key);
        if (values == null) {
            return Collections.emptyList();
        } else {
            return values;
        }
    }

    public boolean put(K key, V value)
    {
        List<V> existing = get(key);
        if (existing.isEmpty()) {
            existing = new ArrayList<>();
            _map.put(key, existing);
        }
        boolean isAdded = existing.add(value);
        if (isAdded) {
            _size++;
        }
        return isAdded;
    }
    
    public int size()
    {
        return _size;
    }
}
