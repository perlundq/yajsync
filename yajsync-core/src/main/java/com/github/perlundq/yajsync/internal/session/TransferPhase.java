/*
 * Sender and Receiver connection state
 *
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
package com.github.perlundq.yajsync.internal.session;

enum TransferPhase
{
    TRANSFER, TEAR_DOWN_1, TEAR_DOWN_2, STOP;

    public TransferPhase next()
    {
        switch (this) {
        case TRANSFER:
            return TEAR_DOWN_1;
        case TEAR_DOWN_1:
            return TEAR_DOWN_2;
        case TEAR_DOWN_2:
            return STOP;
        case STOP:
            return STOP;
        default:
            throw new IllegalStateException();
        }
    }
}
