package com.github.perlundq.yajsync.ui;

import org.junit.Assert;
import org.junit.Test;

public class DictionarySQLTest {

	@Test
	public void testRead() {

		DictionarySQL df = new DictionarySQL();
		df.register("dictionarySQLTest.properties");

		Assert.assertEquals("Lookup value 1", "value1", df.lookup("module1", "user1"));
		Assert.assertEquals("Lookup value 3", "value3", df.lookup(null, "user1"));
		Assert.assertEquals("Lookup value non existent", null, df.lookup(null, "notexistinguser"));
		df.unregister();
	}

}
