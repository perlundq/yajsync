package com.github.perlundq.yajsync.ui;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Dictionary to read configuration values from a database table.
 * 
 * Config file format:
 * <pre>
 * driverClass = com.mysql.jdbc.Driver
 * connectionString = jdbc:mysql://localhost/mydatabase
 * username = myuser
 * password = mypass
 * table = mytable
 * whereModuleField = module
 * whereUserField = user
 * selectValueField = value
 * </pre>
 * 
 * @author Florian Sager, 08.02.2014
 *
 */

public class DictionarySQL implements DictionaryInterface {

	private static String defaultModule = "_default_";

	private Connection conn = null;
	private PreparedStatement pStmt = null;
	private Properties config = new Properties();
	private String filename;

	private static String driverClass = "driverClass";
	private static String connectionString = "connectionString"; 
	private static String username = "username";
	private static String password = "password";
	private static String table = "table";
	private static String whereModuleField = "whereModuleField";
	private static String whereUserField = "whereUserField";
	private static String selectValueField = "selectValueField";

	public void register(String filename) {
		
		this.filename = filename;
		
		try (InputStream is = new FileInputStream(filename)) {
			config.load(is);
		} catch (IOException e) {
			System.err.format("Unable to read SQL dictionary config file %s", filename);
		}

		if (!checkConfigKeyExists(driverClass)
				|| !checkConfigKeyExists(connectionString)
				|| !checkConfigKeyExists(username)
				|| !checkConfigKeyExists(password)
				|| !checkConfigKeyExists(table)
				|| !checkConfigKeyExists(whereModuleField)
				|| !checkConfigKeyExists(whereUserField)
				|| !checkConfigKeyExists(selectValueField)) {
			return;
		}

		try {
			Class.forName (config.getProperty(driverClass)).newInstance();
		} catch (InstantiationException|ClassNotFoundException|IllegalAccessException e) {
			System.err.format("Unable to load SQL driver %s in config file %s: %s", driverClass, filename, e.getMessage());
			return;
		}
		
		try {
			this.conn = DriverManager.getConnection(config.getProperty(connectionString), config.getProperty(username), config.getProperty(password));
		} catch (SQLException e) {
			System.err.format("Unable to create SQL connection for config file %s: %s", filename, e.getMessage());
			return;
		}
		
		StringBuilder buf = new StringBuilder();
		buf.append("SELECT ").append(config.getProperty(selectValueField)).append(" FROM ");
		buf.append(config.getProperty(table)).append(" WHERE ").append(config.getProperty(whereModuleField)).append("=? AND ");
		buf.append(config.getProperty(whereUserField)).append("=?");

		try {
			this.pStmt = this.conn.prepareStatement(buf.toString());
		} catch (SQLException e) {
			System.err.format("Unable to prepare SQL statement '%s' for config file %s: %s", buf.toString(), filename, e.getMessage());
		}
	}
	
	public String lookup(String module, String username) {

		if (this.conn==null || this.pStmt == null) return null;

		if (module == null) {
			module = defaultModule;
		}

		ResultSet rs = null;
		try {
			this.pStmt.setString(1, module);
			this.pStmt.setString(2, username);

			rs = this.pStmt.executeQuery();
			if (!rs.next()) return null;
			return rs.getString(config.getProperty(selectValueField));

		} catch (SQLException e) {
			System.err.format("Unable to execute SQL lookup on module %s and user %s for config file %s: %s", module, username, filename, e.getMessage());
			return null;
		} finally {
			try {
				rs.close();
			} catch (Exception e) { }
		}
	}

	public void unregister() {
		try {
			this.pStmt.close();
			this.conn.close();
		} catch (SQLException e) {
			System.err.format("Unable to close SQL connection related to file %s", filename);
		}
	}

	private boolean checkConfigKeyExists(String key) {
		if (!this.config.containsKey(key)) {
			System.err.format("SQL dictionary %s: the property %s is missing", filename, key);
			return false;
		}
		return true;
	}
}
