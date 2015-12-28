/*
 * Parsing of /etc/rsyncd.conf configuration files
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
package com.github.perlundq.yajsync.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.session.Module;
import com.github.perlundq.yajsync.session.ModuleException;
import com.github.perlundq.yajsync.session.ModuleNotFoundException;
import com.github.perlundq.yajsync.session.ModuleProvider;
import com.github.perlundq.yajsync.session.Modules;
import com.github.perlundq.yajsync.session.RestrictedPath;
import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.Environment;
import com.github.perlundq.yajsync.util.Option;

public class Configuration implements Modules
{
    @SuppressWarnings("serial")
    private static class IllegalValueException extends Exception {}

    public static class Reader extends ModuleProvider {

        private static final Logger _log =
            Logger.getLogger(Reader.class.getName());
        private static final Pattern keyValuePattern =
            Pattern.compile("^([\\w ]+) *= *(\\S.*)$");
        private static final Pattern modulePattern =
            Pattern.compile("^\\[\\s*([\\w ]+)\\s*\\]$");
        private static final String DEFAULT_CONFIGURATION_FILE_NAME =
            "yajsyncd.conf";
        private static final String MODULE_KEY_COMMENT = "comment";
        private static final String MODULE_KEY_PATH = "path";
        private static final String MODULE_KEY_IS_READABLE = "is_readable";
        private static final String MODULE_KEY_IS_WRITABLE = "is_writable";

        private String _cfgFileName =
            Environment.getServerConfig(DEFAULT_CONFIGURATION_FILE_NAME);

        public Reader() {}

        @Override
        public Configuration newAuthenticated(InetAddress address,
                                              Principal principal)
            throws ModuleException
        {
            return newAnonymous(address);
        }

        @Override
        public Configuration newAnonymous(InetAddress address)
            throws ModuleException
        {
            Map<String, Module> modules = getModules(_cfgFileName);
            Configuration cfg = new Configuration(modules);
            return cfg;
        }

        @Override
        public Collection<Option> options()
        {
            Option.ContinuingHandler handler = new Option.ContinuingHandler() {
                @Override public void handleAndContinue(Option option) {
                    _cfgFileName = (String) option.getValue();
                }
            };
            Option o = Option.newStringOption(Option.Policy.OPTIONAL,
                                              "config", "",
                                              String.format("path to " +
                                                            "configuration " +
                                                            "file (default %s)",
                                                            _cfgFileName),
                                              handler);
            List<Option> options = new LinkedList<>();
            options.add(o);
            return options;
        }

        @Override
        public void close()
        {
            // NOP
        }

        private static Map<String, Module> getModules(String fileName)
            throws ModuleException
        {
            Map<String, Map<String, String>> modules;
            try (BufferedReader reader = Files.newBufferedReader(
                                                   Paths.get(fileName),
                                                   Charset.defaultCharset())) {
                modules = parse(reader);
            } catch (IOException e) {
                throw new ModuleException(e);
            }

            Map<String, Module> result = new TreeMap<>();

            for (Map.Entry<String, Map<String, String>> keyVal : modules.entrySet()) {

                String moduleName = keyVal.getKey();
                Map<String, String> moduleContent = keyVal.getValue();

                boolean isGlobalModule = moduleName.isEmpty();
                if (isGlobalModule) {
                    continue;                                                   // Not currently used
                }

                String pathValue = moduleContent.get(MODULE_KEY_PATH);
                boolean isValidModule = pathValue != null;
                if (!isValidModule) {
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format("skipping incomplete " +
                                                   "module %s - lacking path",
                                                   moduleName));
                    }
                    continue;
                }

                try {
                    RestrictedPath vp =
                        new RestrictedPath(Paths.get(moduleName),
                                           Paths.get(pathValue));
                    SimpleModule m = new SimpleModule(moduleName, vp);
                    String comment = Text.nullToEmptyStr(moduleContent.get(MODULE_KEY_COMMENT));
                    m._comment = comment;
                    if (moduleContent.containsKey(MODULE_KEY_IS_READABLE)) {
                        boolean isReadable = toBoolean(moduleContent.get(MODULE_KEY_IS_READABLE));
                        m._isReadable = isReadable;
                    }
                    if (moduleContent.containsKey(MODULE_KEY_IS_WRITABLE)) {
                        boolean isWritable = toBoolean(moduleContent.get(MODULE_KEY_IS_WRITABLE));
                        m._isWritable = isWritable;
                    }
                    result.put(moduleName, m);
                } catch (InvalidPathException | IllegalValueException e) {
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(String.format("skipping module %s: %s",
                                                   moduleName, e.getMessage()));
                    }
                }
            }
            return result;
        }

        private static Map<String, Map<String, String>> parse(BufferedReader reader)
            throws IOException
        {
            String prevLine = "";
            Map<String, Map<String, String>> modules = new TreeMap<>();         // { 'moduleName1' : { 'key1' : 'val1', ..., 'keyN' : 'valN'}, ... }
            Map<String, String> currentModule = new TreeMap<>();                // { 'key1' : 'val1', ..., 'keyN' : 'valN'}
            modules.put("", currentModule);                                     // { 'key1' : 'val1', ..., 'keyN' : 'valN'}
            boolean isEOF = false;

            while (!isEOF) {
                String line = reader.readLine();
                isEOF = line == null;
                if (line == null) {
                    line = "";
                }

                String trimmedLine = prevLine + line.trim();                    // prevLine is non-empty only if previous line ended with a backslash
                if (trimmedLine.isEmpty() || isCommentLine(trimmedLine)) {
                    continue;
                }

                if (!Environment.IS_PATH_SEPARATOR_BACK_SLASH &&
                    trimmedLine.endsWith(Text.BACK_SLASH))
                {
                    prevLine = Text.stripLast(trimmedLine);
                } else {
                    prevLine = "";
                }

                Matcher moduleMatcher = modulePattern.matcher(trimmedLine);
                if (moduleMatcher.matches()) {
                    String moduleName = moduleMatcher.group(1).trim();          // TODO: remove consecutive white space in module name
                    currentModule = modules.get(moduleName);
                    if (currentModule == null) {
                        currentModule = new TreeMap<>();
                        modules.put(moduleName, currentModule);
                    }
                } else {
                    Matcher keyValueMatcher =
                        keyValuePattern.matcher(trimmedLine);
                    if (keyValueMatcher.matches()) {
                        String key = keyValueMatcher.group(1).trim();
                        String val = keyValueMatcher.group(2).trim();
                        currentModule.put(key, val);
                    }
                }
            }
            return modules;
        }

        private static boolean isCommentLine(String line)
        {
            return line.startsWith("#") || line.startsWith(";");
        }

        private static boolean toBoolean(String val) throws IllegalValueException
        {
            if (val == null) {
                throw new IllegalValueException();
            } else if (val.equalsIgnoreCase("true") ||
                       val.equalsIgnoreCase("yes")) {
                return true;
            } else if (val.equalsIgnoreCase("false") ||
                       val.equalsIgnoreCase("no")) {
                return false;
            }
            throw new IllegalValueException();
        }
    }

    private static class SimpleModule implements Module {
        private final String _name;
        private final RestrictedPath _restrictedPath;
        private boolean _isReadable = true;
        private boolean _isWritable = false;
        private String _comment = "";

        public SimpleModule(String name, RestrictedPath restrictedPath) {
            assert name != null;
            assert restrictedPath != null;
            _name = name;
            _restrictedPath = restrictedPath;
        }

        @Override
        public String name() {
            return _name;
        }

        @Override
        public RestrictedPath restrictedPath() {
            return _restrictedPath;
        }

        @Override
        public String comment() {
            return _comment;
        }

        @Override
        public boolean isReadable() {
            return _isReadable;
        }

        @Override
        public boolean isWritable() {
            return _isWritable;
        }
    }

    private final Map<String, Module> _modules;

    public Configuration(Map<String, Module> modules)
    {
        _modules = modules;
    }

    @Override
    public Module get(String moduleName) throws ModuleException
    {
        Module m = _modules.get(moduleName);
        if (m == null) {
            throw new ModuleNotFoundException(
                String.format("module %s does not exist", moduleName));
        }
        return m;
    }

    @Override
    public Iterable<Module> all()
    {
        return _modules.values();
    }
}
