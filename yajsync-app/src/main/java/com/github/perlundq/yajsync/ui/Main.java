/*
 * A rsync client/server command line implementation
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
package com.github.perlundq.yajsync.ui;

import java.io.IOException;
import java.util.Arrays;

public class Main
{
    public static void main(String args[]) throws IOException,
                                                  InterruptedException
    {
        String helpText = "the first argument should be either \"client\" or " +
                          "\"server\"";
        if (args.length == 0) {
            throw new IllegalArgumentException(helpText);
        }

        String[] args2 = Arrays.copyOfRange(args, 1, args.length);
        if (args[0].equals("client")) {
            int rc = new YajsyncClient().start(args2);
            System.exit(rc);
        } else if (args[0].equals("server")) {
            int rc = new YajsyncServer().start(args2);
            System.exit(rc);
        } else {
            throw new IllegalArgumentException(helpText);
        }
    }
}
