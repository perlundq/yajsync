/*
 * Rsync channel tagged message information flag
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
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
package com.github.perlundq.yajsync.channels;

import java.util.HashMap;
import java.util.Map;

public enum MessageCode
{
    DATA         (0),
    ERROR_XFER   (1),
    INFO         (2),   /* remote logging */
    ERROR        (3),   /* remote logging */
    WARNING      (4),   /* remote logging */
    ERROR_SOCKET (5),   /* sibling logging */
    LOG          (6),
    CLIENT       (7),   /* sibling logging */
    ERROR_UTF8   (8),   /* sibling logging */
    REDO         (9),   /* reprocess indicated flist index */
    FLIST        (20),  /* extra file list over sibling socket */
    FLIST_EOF    (21),  /* we've transmitted all the file lists */
    IO_ERROR     (22),  /* the sending side had an I/O error */
    NOOP         (42),  /* a do-nothing message */
    DONE         (86),  /* current phase is done */
    SUCCESS      (100), /* successfully updated indicated flist index */
    DELETED      (101), /* successfully deleted a file on receiving side */
    NO_SEND      (102); /* sender failed to open a file we wanted */

    private final int _value;
    private static final Map<Integer, MessageCode> _map = new HashMap<>();

    static {
        for (MessageCode message : MessageCode.values()) {
            _map.put(message.value(), message);
        }
    }
    
    private MessageCode(int value)
    {
        _value = value;
    }

    public int value()
    {
        return _value;
    }
    
    /**
     * @throws IllegalArgumentException
     */
    public static MessageCode fromInt(int value)
    {
        MessageCode message = _map.get(value);
        if (message == null) {
            throw new IllegalArgumentException(String.format(
                "Error: unknown tag for %d", value));
        }
        return message;
    }
}
