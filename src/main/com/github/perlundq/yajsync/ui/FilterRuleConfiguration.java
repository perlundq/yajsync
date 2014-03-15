/*
 * Rsync filter rules
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

package com.github.perlundq.yajsync.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.github.perlundq.yajsync.filelist.FilterRuleList;
import com.github.perlundq.yajsync.util.ArgumentParsingError;

public class FilterRuleConfiguration {

	private FilterRuleConfiguration parentRuleConfiguration = null;
	private FilterRuleList localRuleList = new FilterRuleList();
	private boolean inheritance = true;
	private String dirMergeFilename = null;
	private String dirname;

	public FilterRuleConfiguration(FilterRuleConfiguration parentRuleConfiguration, String dirname) throws ArgumentParsingError {

		this.parentRuleConfiguration = parentRuleConfiguration;
		if (this.parentRuleConfiguration!=null) {
			this.inheritance = this.parentRuleConfiguration.isInheritance();
			this.dirMergeFilename = this.parentRuleConfiguration.getDirMergeFilename();
		}
		this.dirname = dirname;

		if (dirMergeFilename!=null && (new File(this.dirname+"/"+dirMergeFilename)).exists()) {
			// merge local filter rule file
			readRule(". "+dirname+"/"+dirMergeFilename);
		}
	}

	public void readRule(String plainRule) throws ArgumentParsingError {

		String[] splittedRule = plainRule.split("\\s+");
		if (splittedRule.length!=2) {
			throw new ArgumentParsingError(String.format(
					"failed to parse filter rule '%s', invalid format: should be '<+|-|merge|dir-merge>,<modifier> <filename|path-expression>' in %s",
					plainRule, this.dirname));
		}

		Modifier m = readModifiers(splittedRule[0].trim(), plainRule);
		m.checkValidity(plainRule);

		if (m.merge==true || m.dirMerge==true) {
		
			if (m.noInheritanceOfRules==true) {
				this.inheritance = false;
			}
	
			if (m.merge==true) {
	
				try (BufferedReader br = new BufferedReader(new FileReader(this.dirname+"/"+splittedRule[1].trim()))) {
					String line = br.readLine();
			        while (line != null) {
			        	line = line.trim();
			        	// ignore empty lines or comments
			        	if (line.length()!=0 && !line.startsWith("#")) {
				        	if (m.exclude==true) {
				        		localRuleList.addRule("- "+line);
				        	} else if (m.include==true) {
				        		localRuleList.addRule("+ "+line);
				        	} else {
				        		readRule(line);
				        	}
			        	}
			        	line = br.readLine();
			        }
			    } catch (IOException e) {
			    	throw new ArgumentParsingError(String.format("impossible to parse filter file '%s' for %s", splittedRule[1], this.dirname));
				}
				
				return;
			}
	
			if (this.dirMergeFilename==null && m.dirMerge==true) {
				this.dirMergeFilename = splittedRule[1].trim();
			}
			
			if (m.excludeMergeFilename && this.dirMergeFilename!=null) {
				localRuleList.addRule("- "+this.dirMergeFilename);
			}

			return;
		}

		if (m.exclude==true) {
    		localRuleList.addRule("- "+splittedRule[1].trim());
    		return;
    	} else if (m.include==true) {
    		localRuleList.addRule("+ "+splittedRule[1].trim());
    		return;
    	}

		throw new ArgumentParsingError(String.format("invalid rule %s", plainRule));
	}

	public boolean include(String filename, boolean isDirectory) {

		if (this.localRuleList.include(filename, isDirectory)) {
			return true;
		}

		if (this.parentRuleConfiguration!=null) {

			// search root and check against root only
			FilterRuleConfiguration parent = this;
			while (parent.getParentRuleConfiguration()!=null) {
				parent = parent.getParentRuleConfiguration();
				if (parent.isInheritance()) {
					if (parent.include(filename, isDirectory)) {
						return true;
					}
				}
			}
		}

		return false;
	}

	public FilterRuleConfiguration getParentRuleConfiguration() {
		return parentRuleConfiguration;
	}

	public boolean isInheritance() {
		return inheritance;
	}

	public String getDirMergeFilename() {
		return dirMergeFilename;
	}

	// see http://rsync.samba.org/ftp/rsync/rsync.html --> MERGE-FILE FILTER RULES
	private Modifier readModifiers(String modifier, String plainRule) throws ArgumentParsingError {

		Modifier m = new Modifier();

		int i=0;
		while (i<modifier.length()) {
			
			char c = modifier.charAt(i);

			if (c=='-') {
				// exclude rule
				m.exclude = true;
				i++;
				continue;
			} else if (c=='+') {
				// include rule
				m.include = true;
				i++;
				continue;
			}

			if (i>0) {
				if (c=='e') {
					// exclude the merge-file name from the transfer
					m.excludeMergeFilename = true;
					i++;
					continue;
				} else if (c=='n') {
					// don't inherit rules
					m.noInheritanceOfRules = true;
					i++;
					continue;
				} else if (c=='w') {
					// A w specifies that the rules are word-split on whitespace instead of the normal line-splitting
					throw new ArgumentParsingError(String.format("the modifier 'w' is not implemented, see rule '%s'", plainRule));
				} else if (c==',') {
					i++;
					continue;
				}
			}

			if (c=='.') {
				// merge
				m.merge = true;
				i++;
				continue;
			} else if (c==':') {
				// dir-merge
				m.dirMerge = true;
				i++;
				continue;
			} else if (c=='m' && i+5<=modifier.length() && "merge".equals(modifier.substring(i, i+5))) {
				// merge
				m.merge = true;
				i+=5;
				continue;
			} else if (c=='d' && i+9<=modifier.length() && "dir-merge".equals(modifier.substring(i, i+9))) {
				// dir-merge
				m.dirMerge = true;
				i+=9;
				continue;
			}
			
			throw new ArgumentParsingError(String.format("unknown modifier '%c' in rule %s", c, plainRule));
		}

		return m;
	}

	private class Modifier {
		boolean include;
		boolean exclude;
		boolean excludeMergeFilename;
		boolean noInheritanceOfRules;
		boolean merge;
		boolean dirMerge;

		public void checkValidity(String plainRule) throws ArgumentParsingError {
			if ((merge && dirMerge) || (include && exclude)) {
				throw new ArgumentParsingError(String.format("invalid combination of modifiers in rule %s (processing %s)", plainRule, FilterRuleConfiguration.this.dirname));
			}
		}
	}
}
