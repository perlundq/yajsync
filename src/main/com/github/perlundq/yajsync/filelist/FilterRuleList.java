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
package com.github.perlundq.yajsync.filelist;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.util.ArgumentParsingError;

public class FilterRuleList {

	public List<FilterRule> rules = new ArrayList<FilterRule>();

	public void addRule(String rule) throws ArgumentParsingError {
		rules.add(new FilterRule(rule));
	}

	public boolean include(String filename, boolean isDirectory) {

		for (FilterRule rule : this.rules) {

			if (!isDirectory && rule.isDirectoryOnly()) continue;

			boolean matches = rule.matches(filename);
			
			if (matches) {
				return rule.isInclusion();
			}
		}

		return false;
	}

	/*
	 * see http://rsync.samba.org/ftp/rsync/rsync.html --> FILTER RULES
	 */
	private class FilterRule {

		private boolean inclusion;
		private boolean directoryOnly;
		private boolean absoluteMatching;
		private boolean negateMatching;
		private boolean patternMatching;
		private String path;
		private Pattern pattern;

		/*
		 * @formatter:off
		 * 
			Input samples:
		    + /some/path/this-file-is-found
		    + *.csv
		    - *
		    + !/.svn/

		 * @formatter:on
		 */
		
		public FilterRule(String plainRule) throws ArgumentParsingError {

			String[] splittedRule = plainRule.split("\\s+");
			if (splittedRule.length!=2) {
				throw new ArgumentParsingError(String.format(
						"failed to parse filter rule '%s', invalid format: should be '<+|-> <modifier><path-expression>'",
						plainRule));
			}

			if (!"+".equals(splittedRule[0]) && !"-".equals(splittedRule[0])) {
				throw new ArgumentParsingError(String.format(
	                    "failed to parse filter rule '%s': must start with + (inclusion) or - (exclusion)",
	                    plainRule));
			}

			this.inclusion = "+".equals(splittedRule[0]);

			this.directoryOnly = splittedRule[1].endsWith("/");

			this.negateMatching = splittedRule[1].startsWith("!");

			this.path = splittedRule[1].substring(this.negateMatching ? 1:0, directoryOnly ? splittedRule[1].length()-1:splittedRule[1].length());

			this.absoluteMatching = this.path.startsWith("/");

			// check if string or pattern matching is required
			// this.patternMatching = this.path.contains("*") || this.path.contains("?") || this.path.contains("[");
			this.patternMatching = this.path.matches(".*[\\*\\?\\[].*");

			if (this.patternMatching) {

				StringBuilder b = new StringBuilder();

				if (this.absoluteMatching) {
					b.append("^");
				}

				for (int i=0; i<this.path.length(); i++) {
					
					char c = this.path.charAt(i);

					if (c=='?') {
						b.append("[^/]");
					} else if (c=='*' && i+1<this.path.length() && this.path.charAt(i+1)=='*') {
						b.append(".*");
					} else if (c=='*') {
						b.append("[^/]*");
					} else {
						b.append(c);
					}
				}

				this.pattern = Pattern.compile(b.toString());
			}
		}

		public boolean matches(String filename) {
			
			boolean result;

			if (this.patternMatching) {
				result = this.pattern.matcher(filename).matches();
			} else {
				// string matching
				if (this.absoluteMatching) {
					result = filename.startsWith(this.path);
				} else {
					result = filename.contains(this.path);
				}
			}

			return this.negateMatching ? !result : result;
		}

		public boolean isInclusion() {
			return inclusion;
		}

		public boolean isDirectoryOnly() {
			return directoryOnly;
		}

		public String toString() {
			StringBuilder buf = new StringBuilder();
			buf.append(inclusion ? "+":"-").append(" ");
			buf.append(negateMatching ? "!":"");
			if (patternMatching) {
				buf.append(pattern.toString());
			} else {
				buf.append(path);
			}
			if (directoryOnly) {
				buf.append(" (directory only)");
			}

			return buf.toString();
		}
	}
}
