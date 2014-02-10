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
    // private static final Logger _log = Logger.getLogger(Configuration.class.getName());

    // what is really a correct module name? let's be strict and only allow
    // word characters and whitespace:
    private static final Pattern keyValuePattern =
        Pattern.compile("^([\\w ]+) *= *(\\S.*)$");
    private static final Pattern modulePattern =
        Pattern.compile("^\\[\\s*([\\w ]+)\\s*\\]$");
    private static final Pattern dictionaryPattern =
    	Pattern.compile("^(.*)::(.*)$");
    private final Map<String, Module> _modules = new HashMap<>();
    private final Map<String, Module.Builder> _complete = new HashMap<>();
    private final Map<String, Module.Builder> _incomplete = new HashMap<>();

    // config value names
    public static final String cfgComment = "comment";
    public static final String cfgPath = "path";
    public static final String cfgReadOnly = "read only";
    public static final String cfgAuthUsers = "auth users";
    public static final String cfgSecretMD5 = "secret md5";

    private Configuration() {}

    private void parseContents(BufferedReader reader) throws IOException
    {
        String previousLine = "";
        Module.Builder moduleBuilder =
            new Module.Builder(Module.GLOBAL_MODULE_NAME); // FIXME: not good - the global module is different, it does not have path for example
        boolean doRead = true;
        
        String moduleName = null;

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
                moduleName = moduleMatcher.group(1).trim();              // TODO: remove consecutive white space in module name
                moduleBuilder = restoreOrCreateModule(moduleName);
                continue;
            }

            Matcher keyValueMatcher = keyValuePattern.matcher(trimmedLine);
            if (!keyValueMatcher.matches()) {
                continue;
            }

            String key = keyValueMatcher.group(1).trim();
            String value = keyValueMatcher.group(2).trim();
            ConfigurationValue val;

            Matcher dictionaryMatcher = dictionaryPattern.matcher(value);
            if (dictionaryMatcher.matches()) {
            	String dictionaryType = dictionaryMatcher.group(1).trim();
            	String dictionaryFilename = dictionaryMatcher.group(2).trim();
            	DictionaryInterface dictionary = DictionaryFactory.getDictionary(dictionaryType, dictionaryFilename);
            	val = new ConfigurationDictionaryValue(dictionary);
            } else {
            	val = new ConfigurationValue(value);
            }

            if (key.equals(cfgComment)) {             // NOTE: will happily overwrite any previous set value
                moduleBuilder.setComment(val);
            } else if (key.equals(cfgPath)) {
               	moduleBuilder.setPath(val);
            } else if (key.equals(cfgReadOnly)) {
            	moduleBuilder.setIsReadOnly(val);
            } else if (key.equals(cfgAuthUsers)) {
            	moduleBuilder.setAuthUsers(val);
            } else if (key.equals(cfgSecretMD5)) {
            	moduleBuilder.setSecretMD5(val);
            }
        }

        saveModule(moduleBuilder);
        // buildAllModules();
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

    // generate user-specific configuration
    public Map<String, Module> modules(String username) {
    	
    	Map<String, Module> userModules = new HashMap<>();
    	
        for (Map.Entry<String, Module.Builder> entry : _complete.entrySet()) {
        	userModules.put(entry.getKey(), new Module(entry.getValue(), username));
        }

        return userModules;
    }

    /* private void buildAllModules() {
        for (Map.Entry<String, Module.Builder> entry : _complete.entrySet()) {
            _modules.put(entry.getKey(), new Module(entry.getValue()));
        }
        _complete.clear();
        _incomplete.clear();
    } */

    private static boolean isCommentLine(String line)
    {
        return line.startsWith("#") || line.startsWith(";");
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
