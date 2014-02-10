package com.github.perlundq.yajsync.ui;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory to register, retrieve and unregister dictionaries
 * 
 * @author Florian Sager, 10.02.2014
 *
 */

public class DictionaryFactory {

	private static Map<DictionaryKey, DictionaryInterface> registeredDictionaries = new HashMap<DictionaryKey, DictionaryInterface>();

	public static DictionaryInterface getDictionary(String dictionaryType, String filename) {

		DictionaryKey key = new DictionaryFactory().new DictionaryKey(dictionaryType, filename);
		DictionaryInterface dictionary = registeredDictionaries.get(key);
		if (dictionary == null) {
			try {
				dictionary = (DictionaryInterface) Class.forName("com.github.perlundq.yajsync.ui.Dictionary"+dictionaryType).newInstance();
				dictionary.register(filename);
				registeredDictionaries.put(key, dictionary);
			} catch (InstantiationException|ClassNotFoundException|IllegalAccessException e) {
				System.err.format("Unable to load class 'Dictionary%s'", dictionaryType, e.getMessage());
			}
		}
		return dictionary;
	}

	public static void unregisterDictionaries() {
		for (DictionaryInterface dictionary : registeredDictionaries.values()) {
			dictionary.unregister();
		}
	}

	private class DictionaryKey {

		private String dictionaryType;
		private String filename;

		public DictionaryKey(String dictionaryType, String filename) {
			this.dictionaryType = dictionaryType;
			this.filename = filename;
		}

		public String getDictionaryType() {
			return dictionaryType;
		}

		public String getFilename() {
			return filename;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof DictionaryKey)) return false;
			DictionaryKey otherKey = (DictionaryKey) o;
			return this.getDictionaryType().equals(otherKey.getDictionaryType()) && this.getFilename().equals(otherKey.getFilename());
		}
	}
}
