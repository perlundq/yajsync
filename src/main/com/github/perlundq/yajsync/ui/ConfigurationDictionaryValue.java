package com.github.perlundq.yajsync.ui;

/**
 * Container for a value that is fetched from a dictionary at runtime, e.g. configure
 * <pre>
 * name1 = File::/app/name1file.txt
 * name2 = SQL::/app/name2sql.cfg
 * </pre>
 * 
 * @author Florian Sager, 10.02.2014
 *
 */

public class ConfigurationDictionaryValue extends ConfigurationValue {
	
	private DictionaryInterface dictionary;

	public ConfigurationDictionaryValue(DictionaryInterface dictionary) {
		super(dictionary);
		this.dictionary = dictionary;
	}

	public String getValue(String modulename, String username) {
		return this.dictionary.lookup(modulename, username);
	}
}
