package com.github.perlundq.yajsync.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Dictionary to read configuration values from a file.
 * 
 * File format:
 * <pre>
 * module1:user1:value1
 * module1:user2:value2
 * _default_:user1:value3
 * </pre>
 * 
 * @author Florian Sager, 08.02.2014
 * 
 */

public class DictionaryFile implements DictionaryInterface {

	private static String defaultModule = "_default_";
	private static String valueSeparator = ":";
	private static String commentPrefix = "#";

	private Map<String, String> dictionary = new HashMap<String, String>();

	public void register(String filename) {

		try (BufferedReader br = new BufferedReader(
				new FileReader(new File(filename)))) {

			String line;
			int lineCount = 0;

			while ((line = br.readLine()) != null) {

				String trimmedLine = line.trim();
				if (trimmedLine.startsWith(commentPrefix) || trimmedLine.isEmpty()) {
					continue;
				}

				String[] splittedValues = trimmedLine.split(valueSeparator, 3);
				if (splittedValues.length<3) {
					System.err.format("Dictionary file %s:%i: invalid values, see %s", filename, lineCount, trimmedLine);
					continue;
				}
				
				String hashKey = getHashKey(splittedValues[0], splittedValues[1]);
				
				if (dictionary.containsKey(hashKey)) {
					System.err.format("Dictionary file %s:%i: module %s, user %s, duplicate definition is ignored", filename, lineCount, splittedValues[0], splittedValues[1]);
					continue;
				}

				dictionary.put(hashKey, splittedValues[2]);

				lineCount++;
			}

		} catch (FileNotFoundException e) {
			System.err.format("Dictionary file %s not found", filename);
		} catch (IOException e) {
			System.err.format("Unable to read dictionary file %s", filename);
		}
	}

	public String lookup(String module, String username) {
		if (module == null) {
			module = defaultModule;
		}
		return this.dictionary.get(this.getHashKey(module, username));
	}

	public void unregister() { }

	private String getHashKey(String module, String username) {
		return module + ":" + username;
	}
}
