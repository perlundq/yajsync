package com.github.perlundq.yajsync.ui;

/**
 * Generic interface for flexible configuration dictionaries
 *  
 * @author Florian Sager, 08.02.2014
 */

public interface DictionaryInterface {

	public void register(String filename);
	public String lookup(String module, String username);
	public void unregister();

}
