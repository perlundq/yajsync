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
package com.github.perlundq.yajsync.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class ProcessRunner {

    public class Result {
        public final String _stdout;
        public final String _stderr;
        public final int _exitcode;
        private final String _command;

        public Result(String out, String err, int exitcode, String command) {
            _stdout = out;
            _stderr = err;
            _exitcode = exitcode;
            _command = command;
        }

        public String getCommand() {
            return _command;
        }

        public ProcessException getProcessException() {
            return new ProcessException(
                    "CMD: " + _command + "; EXITCODE: " + _exitcode
                            + "; STDOUT: " + _stdout + "; STDERR: " + _stderr);
        }
    }

    public class ProcessException extends IOException {

        private static final long serialVersionUID = 1L;
        String _msg;

        ProcessException(String msg) {
            _msg = msg;
        }

        @Override
        public String getMessage() {
            return _msg;
        }
    }

    public static Result exec(List<String> commandList) throws IOException {

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

        ProcessRunner pr = new ProcessRunner();

        StringBuilder buf = new StringBuilder();
        for (String cmd : commandList) {
            buf.append(cmd).append(" ");
        }
        String command = buf.toString().trim();

        Process process = Runtime.getRuntime().exec(command);
        InputStreamTransformer stdoutReader = pr.new InputStreamTransformer(
                process.getInputStream(), stdout);
        InputStreamTransformer stderrReader = pr.new InputStreamTransformer(
                process.getErrorStream(), stderr);

        try {
            process.waitFor();
            stdoutReader.join();
            stderrReader.join();
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }

        return pr.new Result(stdout.toString(), stderr.toString(),
                process.exitValue(), command);
    }

    class InputStreamTransformer extends Thread {

        private final InputStream _mIn;

        private final OutputStream _mOut;

        public InputStreamTransformer(InputStream in, OutputStream out) {
            _mIn = in;
            _mOut = out;
            start();
        }

        @Override
        public void run() {
            try {
                int c;
                while ((c = _mIn.read()) != -1) {
                    _mOut.write((char) c);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
