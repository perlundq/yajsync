package com.github.perlundq.yajsync.ui;

/**
 * Default configuration value, e.g.
 * <pre>
 * name = value
 * </pre>
 * 
 * @author Florian Sager, 10.02.2014
 *
 */

public class ConfigurationValue {

	private Object value;

	public ConfigurationValue(Object v) {
		this.value = v;
	}
	
	public String getValue(String modulename, String username) {
		return this.value==null ? null : this.value.toString();
	}
}
