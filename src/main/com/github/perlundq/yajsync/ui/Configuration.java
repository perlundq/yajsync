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
package com.github.perlundq.yajsync.ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.text.Text;
import com.github.perlundq.yajsync.util.Environment;

public class Configuration
{
    // what is really a correct module name? let's be strict and only allow
    // word characters and whitespace:
    private static final Pattern keyValuePattern =
        Pattern.compile("^([\\w ]+) *= *(\\S.*)$");
    private static final Pattern modulePattern =
        Pattern.compile("^\\[\\s*([\\w ]+)\\s*\\]$");
    private final Map<String, Module> _modules = new HashMap<>();
    private final Map<String, Module.Builder> _complete = new HashMap<>();
    private final Map<String, Module.Builder> _incomplete = new HashMap<>();

    private Configuration() {}

    private void parseContents(BufferedReader reader) throws IOException
    {
        String previousLine = "";
        Module.Builder moduleBuilder =
            new Module.Builder(Module.GLOBAL_MODULE_NAME); // FIXME: not good - the global module is different, it does not have path for example
        boolean doRead = true;

        while (doRead) {
            String line = reader.readLine();
            if (line == null) {
                line = "";
                doRead = false;
            }

            String trimmedLine = line.trim();
            if (!previousLine.isEmpty()) {
                trimmedLine = previousLine + trimmedLine;
            }

            if (!Environment.IS_PATH_SEPARATOR_BACK_SLASH &&
                trimmedLine.endsWith(Text.BACK_SLASH)) 
            {
                previousLine = Text.stripLast(trimmedLine);
            } else {
                previousLine = "";
            }

            if (trimmedLine.isEmpty() || isCommentLine(trimmedLine)) {
                continue;
            }

            Matcher moduleMatcher = modulePattern.matcher(trimmedLine);
            if (moduleMatcher.matches()) {
                saveModule(moduleBuilder);
                String moduleName = moduleMatcher.group(1).trim();              // TODO: remove consecutive white space in module name
                moduleBuilder = restoreOrCreateModule(moduleName);
                continue;
            }

            Matcher keyValueMatcher = keyValuePattern.matcher(trimmedLine);
            if (!keyValueMatcher.matches()) {
                continue;
            }

            String key = keyValueMatcher.group(1).trim();
            String val = keyValueMatcher.group(2).trim();

            // FIXME: don't hardcode these:
            if (key.equals("comment")) {             // NOTE: will happily overwrite any previous set value
                moduleBuilder.setComment(val);
            } else if (key.equals("path")) {
                try {
                    moduleBuilder.setPath(Paths.get(val));
                } catch (IOException e) {
                    System.err.format("Error: failed to set module path to %s" +
                                      " for module %s%n", val, moduleBuilder.name());
                }
            } else if (key.equals("read only")) {
                String booleanString = toBooleanStringOrNull(val);
                if (booleanString != null) {
                    moduleBuilder.setIsReadOnly(Boolean.valueOf(val));
                } else {
                    System.err.format(
                        "Error: illegal value for module parameter " +
                        "'read only': %s (expected either of " +
                        "true/false/yes/no/1/0)", val);
                }
            }
        }

        saveModule(moduleBuilder);
        buildAllModules();
    }

    public static Configuration readFile(String fileName) throws IOException
    {
        try (BufferedReader reader =
                Files.newBufferedReader(Paths.get(fileName),
                                        Charset.defaultCharset())) {

            Configuration instance = new Configuration();
            instance.parseContents(reader);
            return instance;
        }
    }

    public Module getGlobalModule()
    {
        return _modules.get(Module.GLOBAL_MODULE_NAME);
    }

    public Map<String, Module> modules()
    {
        return _modules;
    }

    private void buildAllModules()
    {
        for (Map.Entry<String, Module.Builder> entry : _complete.entrySet()) {
            _modules.put(entry.getKey(), new Module(entry.getValue()));
        }
        _complete.clear();
        _incomplete.clear();
    }

    private static boolean isCommentLine(String line)
    {
        return line.startsWith("#") || line.startsWith(";");
    }

    private static String toBooleanStringOrNull(String val)
    {
        if (val.equalsIgnoreCase("true") ||
            val.equalsIgnoreCase("1") ||
            val.equalsIgnoreCase("yes")) {
            return "true";
        } else if (val.equalsIgnoreCase("false") ||
                   val.equalsIgnoreCase("0") ||
                   val.equalsIgnoreCase("no")) {
            return "false";
        }
        return null;
    }

    private Module.Builder restoreOrCreateModule(String moduleName)
    {
        if (_modules.containsKey(moduleName)) {
            return _complete.remove(moduleName);
        } else if (_incomplete.containsKey(moduleName)){
            return _incomplete.remove(moduleName);
        } else {
            return new Module.Builder(moduleName);
        }
    }

    private void saveModule(Module.Builder builder) {
        if (builder.isGlobal() || builder.hasPath()) {
            _complete.put(builder.name(), builder);
        } else {
            _incomplete.put(builder.name(), builder);
        }
    }
}
