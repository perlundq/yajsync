package com.github.perlundq.yajsync.ui;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.BeforeClass;
import org.junit.Test;

import com.github.perlundq.yajsync.util.ArgumentParsingError;

public class FilterRuleConfigurationTest {

	private static String rootDirectory;
	private static String mergeFile;

	@BeforeClass
	public static void beforeClass() throws IOException {
		File f = File.createTempFile(".rsyncMerge", ".filter");
		rootDirectory = f.getParentFile().getAbsolutePath();
		mergeFile = f.getName();
		
		PrintWriter writer = new PrintWriter(f);
		writer.println("# rsync filter file");
		writer.println("");
		writer.println("+ abc");
		writer.println("- def");
		writer.close();
		
		f.deleteOnExit();
	}

	@Test
	public void test1() throws ArgumentParsingError {
		
		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("+ test");

		assertEquals(true, cfg.include("test", false));
	}
	
	@Test
	public void test2() throws ArgumentParsingError {

		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("+ test");
		cfg.readRule(":e .rsyncInclude-not-exists");

		assertEquals(true, cfg.include("test", false));
	}
	
	@Test
	public void test3() throws ArgumentParsingError {

		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("+ test");
		cfg.readRule(":e "+mergeFile);

		assertEquals(true, cfg.include("test", false) && !cfg.include(mergeFile, false));
	}
	
	@Test
	public void test3a() throws ArgumentParsingError {

		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("+ test");
		cfg.readRule("dir-merge "+mergeFile);

		assertEquals(true, cfg.include("test", false) && !cfg.include(mergeFile, false));
	}	
	
	@Test
	public void test4() throws ArgumentParsingError {

		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("+ test");
		cfg.readRule(".e "+mergeFile);

		assertEquals(true, cfg.include("test", false) && !cfg.include(mergeFile, false) && cfg.include("abc", false) && !cfg.include("def", false));
	}
	
	@Test
	public void test4a() throws ArgumentParsingError {

		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("+ test");
		cfg.readRule("merge,e "+mergeFile);

		assertEquals(true, cfg.include("test", false) && !cfg.include(mergeFile, false) && cfg.include("abc", false) && !cfg.include("def", false));
	}
	
	@Test
	public void test5() throws ArgumentParsingError {

		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("+ test");
		cfg.readRule("merge,n "+mergeFile);

		FilterRuleConfiguration subCfg = new FilterRuleConfiguration(cfg, rootDirectory);
		subCfg.readRule("+ test");

		assertEquals(true, cfg.include("test", false) && !cfg.include(mergeFile, false) && cfg.include("abc", false) && !cfg.include("def", false)
				&& subCfg.include("test", false) && !subCfg.include("abc", false) && !subCfg.include("def", false));
	}
	
	@Test
	public void test5a() throws ArgumentParsingError {

		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("+ test");
		cfg.readRule("merge "+mergeFile);

		FilterRuleConfiguration subCfg = new FilterRuleConfiguration(cfg, rootDirectory);
		subCfg.readRule("+ test");

		assertEquals(true, cfg.include("test", false) && !cfg.include(mergeFile, false) && cfg.include("abc", false) && !cfg.include("def", false)
				&& subCfg.include("test", false) && subCfg.include("abc", false) && !subCfg.include("def", false));
	}
	
	@Test
	public void test6() throws ArgumentParsingError {

		FilterRuleConfiguration cfg = new FilterRuleConfiguration(null, rootDirectory);
		cfg.readRule("merge "+mergeFile);
		cfg.readRule("+ *");

		FilterRuleConfiguration subCfg = new FilterRuleConfiguration(cfg, rootDirectory);
		subCfg.readRule("+ test");

		assertEquals(true, cfg.include("test", false) && cfg.include(mergeFile, false) && cfg.include("abc", false) && !cfg.include("def", false)
				&& subCfg.include("test", false) && subCfg.include("ghi", false) && !subCfg.include("def", false));
	}	
}
