/*
 * Flags used for caching of previous sent file meta data between
 * Sender and Receiver
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013-2016 Per Lundqvist
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
package com.github.perlundq.yajsync.internal.session;

final class TransmitFlags
{
    private TransmitFlags() {}

    static final int TOP_DIR            = 1 << 0;
    static final int SAME_MODE          = 1 << 1;
    static final int EXTENDED_FLAGS     = 1 << 2;      /* protocols 28 - now */
    static final int SAME_UID           = 1 << 3;
    static final int SAME_GID           = 1 << 4;
    static final int SAME_NAME          = 1 << 5;
    static final int LONG_NAME          = 1 << 6;
    static final int SAME_TIME          = 1 << 7;
    static final int SAME_RDEV_MAJOR    = 1 << 8;     /* protocols 28 - now (devices only) */
    static final int USER_NAME_FOLLOWS  = 1 << 10;  /* protocols 30 - now */
    static final int GROUP_NAME_FOLLOWS = 1 << 11; /* protocols 30 - now */
    static final int IO_ERROR_ENDLIST   = 1 << 12;   /* protocols 31*- now (w/EXTENDED_FLAGS) (also protocol 30 w/'f' compat flag) */
    //final static int SAME_RDEV_pre28 = (1<<2);     /* protocols 20 - 27  */

    // static final int NO_CONTENT_DIR = (1<<8);      /* protocols 30 - now (dirs only) */
    // static final int HLINKED = (1<<9);             /* protocols 28 - now */
    // static final int SAME_DEV_pre30 = (1<<10);     /* protocols 28 - 29  */
    //final  static int RDEV_MINOR_8_pre30 = (1<<11); /* protocols 28 - 29  */
    //final  static int HLINK_FIRST = (1<<12);        /* protocols 30 - now (HLINKED files only) */
}
