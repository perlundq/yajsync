/*
 * /etc/rsyncd.conf module information
 *
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

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import com.github.perlundq.yajsync.session.RsyncSecurityException;
import com.github.perlundq.yajsync.util.PathOps;

public class Module
{
	public static class Builder {

		private String _name = GLOBAL_MODULE_NAME;

		private ConfigurationValue _comment = new ConfigurationValue(null);
        private ConfigurationValue _path = new ConfigurationValue(null);
        private ConfigurationValue _isReadOnly = new ConfigurationValue("false");
        private ConfigurationValue _authUsers = new ConfigurationValue(null);
        private ConfigurationValue _secrets = new ConfigurationValue(null);
        
        private boolean isSetAuthUsers = false;

        /**
         * @throws IllegalArgumentException
         */
        Builder(String name) {
            if (name == null) {
                throw new IllegalArgumentException("supplied module name is null");
            }
            _name = name;
        }

        public boolean isGlobal() {
        	return _name.equals(GLOBAL_MODULE_NAME);
        }
        
        public String name() {
            return _name;
        }

        public void setIsReadOnly(ConfigurationValue isReadOnly) {
            _isReadOnly = isReadOnly;
        }

        /**
         * @throws IllegalArgumentException
         */
        public void setComment(ConfigurationValue comment) {
            if (comment == null) {
                throw new IllegalArgumentException("comment is null");
            }
            _comment = comment;
        }

        /**
         * @throws IllegalArgumentException
         */
        public void setPath(ConfigurationValue path) throws IOException {
            if (path == null) {
                throw new IllegalArgumentException("path is null");
            }
            this._path = path;
        }

        public boolean hasPath() {
            return _path != null;
        }

        public void setAuthUsers(ConfigurationValue authUsers) {
        	this._authUsers = authUsers;
        	this.isSetAuthUsers = true;
        }
        
        public void setSecrets(ConfigurationValue secrets) {
        	this._secrets = secrets;
        }

        public boolean isAuthenticationRequired() {
        	return this.isSetAuthUsers;
        }

        @Override
        public String toString() {
            return String.format("%s (name=%s comment=%s path=%s " +
                                 "isReadOnly=%s authUsers=%s secrets=%s)",
                                 getClass().getSimpleName(), _name, _comment,
                                 _path, _isReadOnly, _authUsers, _secrets);
        }
    }

	public static final String GLOBAL_MODULE_NAME = "";
    private final String modulename;
    private final String username;
    private final String comment;
    private final Path path;
    private final Boolean isReadOnly;
    private final Boolean isUserMatching;
    private final String secrets;

    public Module(Builder builder, String _username)
    {
    	// user specific initialization
    	this.modulename = builder._name;
    	this.username = _username;
    	this.comment = convertComment(builder._comment);
        this.path = convertPath(builder._path);
        this.isReadOnly = convertIsReadOnly(builder._isReadOnly);
        this.isUserMatching = convertAuthUsers(builder._authUsers);
        this.secrets = convertSecrets(builder._secrets);
    }

    public boolean isGlobal() {
        return modulename.equals(GLOBAL_MODULE_NAME);
    }
    
    public String name() {
        return modulename;
    }

    public String comment() {
        return comment;
    }
    
    public String convertComment(ConfigurationValue v) {
    	return v.getValue(modulename, username);
    }
    
    public Path convertPath(ConfigurationValue v) {
    	
    	String s = v.getValue(modulename, username);
    	if (s==null && isGlobal()) return null;

    	Path path = null;
    	try {
    		path = Paths.get(s).toRealPath(LinkOption.NOFOLLOW_LINKS);
    		if (path.getNameCount() == 0) {
    			// System.err.format("Error: module path may not be the root directory for module %s%n", this.modulename);
    			// return null;
    			throw new IllegalArgumentException(String.format("Error: module path may not be the root directory for module %s%n", this.modulename));
	        }
    	} catch (NullPointerException|IOException e) {
    		// System.err.format("Error: failed to set module path to %s for module %s%n", s, this.modulename);
    		// return null;
    		throw new IllegalArgumentException(String.format("Error: failed to set module path to %s for module %s%n", s, this.modulename));
    	}

    	return path;
    }

    public boolean isReadOnly() {
    	return this.isReadOnly;
    }

    public Boolean convertIsReadOnly(ConfigurationValue v) {
    	String s = v.getValue(this.modulename, username);
    	Boolean isReadOnly = toBooleanStringOrNull(s);
    	if (isReadOnly == null) {
            System.err.format(
                "Error: illegal value for module parameter " +
                "'%s': %s (expected either of " +
                "true/false/yes/no/1/0)", Configuration.cfgReadOnly, s);
            isReadOnly = true;
    	}
    	return isReadOnly;
    }

    public boolean isUserMatching() {
    	return this.isUserMatching;
    }

    public Boolean convertAuthUsers(ConfigurationValue v) {

    	// look for a matching of the current user in one or multiple comma separated usernames
    	String authUsers = v.getValue(modulename, username);
    	
    	// public access if 'auth users' is not defined
    	if (authUsers == null) return true;
    	
    	// no access if username is null
    	if (username == null) return false;

    	String[] splittedAuthUsers = authUsers.split(",");
    	for (String u : splittedAuthUsers) {
    		if (this.username.equals(u.trim())) {
    			return true;
    		}
    	}
    	return false;
    }

    public String getSecrets() {
    	return this.secrets;
    }

    public String convertSecrets(ConfigurationValue v) {
    	return v.getValue(modulename, username);
    }

    public Path resolveVirtual(Path other) {

    	Path normalized = normalizeEmptyToDotDir(other);                        // e.g. MODULE/path/to/file
        Path moduleNameAsPath = Paths.get(modulename);
        if (!normalized.startsWith(moduleNameAsPath)) {                         // NOTE: any absolute paths will throw here, as will MODULE/..
            throw new RsyncSecurityException(String.format(
                "\"%s\" is outside module virtual top dir %s", other, modulename));
        }

        int moduleNameCount = moduleNameAsPath.getNameCount();
        if (normalized.getNameCount() == moduleNameCount) {
            return path;
        }
        Path strippedOfModulePrefix =
            normalized.subpath(moduleNameCount,
                               normalized.getNameCount());
        return resolve(strippedOfModulePrefix);
    }

    // NOTE: may return SAFE/PATH/TO/MODULE_TOP_DIR/.. 
    private Path resolve(Path other)
    {
        Path result = resolveOrNull(other);
        if (result == null) {
            throw new RsyncSecurityException(String.format(
                "%s is outside module top dir %s", other, path));
        }
        return result;
    }

    // NOTE: may return SAFE/PATH/TO/MODULE_TOP_DIR/.. 
    private Path resolveOrNull(Path other)
    {
        Path normalized = normalizeEmptyToDotDir(other);
        if (!normalized.isAbsolute()) {
            normalized = path.resolve(normalized);
        }
        if (normalized.startsWith(path)) {
            return normalized;
        }
        return null;
    }

    // NOTE: might return path prefixed with ..
    private static Path normalizeEmptyToDotDir(Path path)
    {
        Path normalized = path.normalize();
        if (normalized.equals(PathOps.EMPTY)) {
            return PathOps.DOT_DIR;
        }
        return normalized;
    }

    private static Boolean toBooleanStringOrNull(String val)
    {
        if (val.equalsIgnoreCase("true") ||
            val.equalsIgnoreCase("1") ||
            val.equalsIgnoreCase("yes")) {
            return true;
        } else if (val.equalsIgnoreCase("false") ||
                   val.equalsIgnoreCase("0") ||
                   val.equalsIgnoreCase("no")) {
        	return false;
        }
        return null;
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && getClass() == other.getClass()) {
            Module otherModule = (Module) other;
            return name().equals(otherModule.name());
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(modulename);
    }

    @Override
    public String toString() {
        // TODO: print only non-null values
        return String.format("[%s]:%n\tcomment = %s\n\tpath = %s\n\tread only = %s\n\tauth users = %s%n",
                             modulename, comment, path, isReadOnly, isUserMatching ? username : "<no match>");
    }    
}
