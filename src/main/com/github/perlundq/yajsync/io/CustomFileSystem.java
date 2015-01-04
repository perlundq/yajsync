/*
 * Configurable FileSystem implementations
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014 Per Lundqvist
 * Copyright (C) 2014 Florian Sager
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
package com.github.perlundq.yajsync.io;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;


public class CustomFileSystem {

	private static FileSystem _filesystem = FileSystems.getDefault();
	private static FileSystem _configFilesystem = FileSystems.getDefault();
	private static FileSystem _tempFilesystem = FileSystems.getDefault();

	public static void setFileSystem(FileSystem filesystem) {
		_filesystem = filesystem;
	}
	
	public static void setConfigFileSystem(FileSystem configFilesystem) {
		_configFilesystem = configFilesystem;
	}
	
	public static void setTempFileSystem(FileSystem tempFilesystem) {
		_tempFilesystem = tempFilesystem;
	}

	public static Path getPath(String path) {
		return _filesystem.getPath(path);
	}

	public static Path getConfigPath(String path) {
		return _configFilesystem.getPath(path);
	}

	public static Path getTempPath(String path) {
		return _tempFilesystem.getPath(path);
	}
}
