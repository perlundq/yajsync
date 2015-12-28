/*
 * Thread safe rsync file information list
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
package com.github.perlundq.yajsync.filelist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConcurrentFilelist extends Filelist
{
    public ConcurrentFilelist(boolean isRecursive, boolean isPruneDuplicates)
    {
        super(isRecursive,
              isPruneDuplicates,
              Collections.synchronizedList(new ArrayList<Segment>()));
    }

    @Override
    public String toString()
    {
        synchronized(_segments) {
            return super.toString();
        }
    }

    @Override
    public Segment newSegment(SegmentBuilder builder)
    {
        return super.newSegment(builder,
                                new ConcurrentSkipListMap<Integer, FileInfo>());
    }

    @Override
    public Segment getSegmentWith(int fileIndex)
    {
        assert fileIndex >= 0;
        synchronized(_segments) {
            return super.getSegmentWith(fileIndex);
        }
    }

    @Override
    public Segment deleteFirstSegment()
    {
        synchronized(_segments) {
            return super.deleteFirstSegment();
        }
    }
}
