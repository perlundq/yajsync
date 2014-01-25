/*
 * Current Sender and Receiver connection state
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
package com.github.perlundq.yajsync.session;

class ConnectionState
{
    private final static int LAST_PHASE = 3;
    private int _currentPhase = 0;
    
    @Override
    public String toString()
    {
        String state;
        switch (_currentPhase) {
        case 0:
            state = "transfer";
            break;
        case 1:
        case 2:
            state = "tearing down " + _currentPhase;
            break;
        case 3:
            state = "stopped";
            break;
        default:
            throw new IllegalStateException(String.format("illegal phase (%d)",
                                                          _currentPhase));    
        }
        return String.format("%s %s", getClass().getSimpleName(), state);
    }
    
    public boolean isTearingDown()
    {
        return _currentPhase > 0;
    }

    public boolean isTransfer()
    {
        return _currentPhase < LAST_PHASE;
    }
     
    public void doTearDownStep()
    {
        if (_currentPhase < LAST_PHASE) {
            _currentPhase++;
        } else {
            throw new IllegalStateException("already at last phase");
        }
    }
}
