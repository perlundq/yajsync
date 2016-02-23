/*
 * Copyright (C) 2016 Florian Sager
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.perlundq.yajsync.session.SessionConfig;
import com.github.perlundq.yajsync.session.Statistics;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.ProcessRunner;
import com.github.perlundq.yajsync.util.ProcessRunner.Result;

public class RsyncClient implements SyncClient {

    private final String _whichCommand = "which";
    private final String _rsyncCommand = "rsync";
    private String _rsyncBinary;

    private Statistics _statistics;

    public RsyncClient() {
        if (!Environment.IS_POSIX_FS) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " test cannot be executed on this system");
        }

        try {
            Result whichRsync = ProcessRunner
                    .exec(Arrays.asList(_whichCommand, _rsyncCommand));
            if (whichRsync._exitcode == 0) {
                _rsyncBinary = whichRsync._stdout.trim();
            } else {
                throw new UnsupportedOperationException(
                        "no rsync binary found on this system");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int start(String[] args) {

        _statistics = new Statistics();

        List<String> command = new ArrayList<String>();
        command.addAll(Arrays.asList(_rsyncBinary,
                "--protocol=" + SessionConfig.VERSION.major(), "--stats"));
        command.addAll(Arrays.asList(args));

        try {
            Result rsyncResult = ProcessRunner.exec(command);
            if (rsyncResult._exitcode == 0) {
                _statistics.setFileListBuildTime(parseStatTime(
                        "File list generation time: ", rsyncResult._stdout));
                _statistics.setFileListTransferTime(parseStatTime(
                        "File list transfer time: ", rsyncResult._stdout));
                _statistics.setNumFiles(
                        parseStatInt("Number of files: ", rsyncResult._stdout));
                _statistics.setNumTransferredFiles(
                        parseStatInt("Number of regular files transferred: ",
                                rsyncResult._stdout));
                _statistics.setTotalFileListSize(
                        parseStatInt("File list size: ", rsyncResult._stdout));
                _statistics.setTotalFileSize(
                        parseStatInt("Total file size: ", rsyncResult._stdout));
                _statistics.setTotalLiteralSize(
                        parseStatInt("Literal data: ", rsyncResult._stdout));
                _statistics.setTotalMatchedSize(
                        parseStatInt("Matched data: ", rsyncResult._stdout));
                _statistics.setTotalRead(parseStatInt("Total bytes received: ",
                        rsyncResult._stdout));
                _statistics.setTotalTransferredSize(parseStatInt(
                        "Total transferred file size: ", rsyncResult._stdout));
                _statistics.setTotalWritten(parseStatInt("Total bytes sent: ",
                        rsyncResult._stdout));
            }

            return rsyncResult._exitcode;

        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /*
     * Number of files: 1 (reg: 1) Number of created files: 1 (reg: 1) Number of
     * regular files transferred: 1 Total file size: 512 bytes Total transferred
     * file size: 512 bytes Literal data: 512 bytes Matched data: 0 bytes File
     * list size: 0 File list generation time: 0.001 seconds File list transfer
     * time: 0.000 seconds Total bytes sent: 619 Total bytes received: 38
     */

    private int parseStatInt(String prefix, String s) {
        String value = parseStatString(prefix, s);
        if (value == null) {
            return -1;
        }
        return Integer.parseInt(value.replaceAll("[\\D]", ""));
    }

    private long parseStatTime(String prefix, String s) {
        String value = parseStatString(prefix, s);
        if (value == null) {
            return -1;
        }
        return (long) (Double.parseDouble(value) * 1000d);
    }

    private String parseStatString(String prefix, String s) {
        int beginIndex = s.indexOf(prefix);
        if (beginIndex > 0) {
            int endIndex = s.indexOf("\n", beginIndex + prefix.length());
            if (endIndex < 0) {
                endIndex = s.length() - 1;
            }
            int spaceIndex = s.indexOf(" ", beginIndex + prefix.length());
            if (spaceIndex > 0 && spaceIndex < endIndex) {
                endIndex = spaceIndex;
            }
            return s.substring(beginIndex + prefix.length(), endIndex);
        }

        return null;
    }

    @Override
    public Statistics statistics() {
        return _statistics;
    }

}
