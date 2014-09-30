General
-------
Code must be compatible with GPL version 3 or later and must include a
proper GPL header with proper copyright attribution.

Write unit tests for anything non-trivial.


Git
---
- Configure your repository with your name and email address, also
  configure core.whitespace with
  `trailing-space,space-before-tab,tab-in-indent,tabwidth=4`.
- Fix all whitespace errors with `git diff --check` before committing.
- Write well formatted commits
  http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
- Feel free to clean up your commits (e.g. amend/squash/rebase) if
  that makes code reviewing easier.
- Please create a new branch for your change. Any unrelated change
  belongs to a separate branch and a separate pull request.
- Run `ant test` to verify it builds OK and no tests fail before
  sending a pull request.
- When sending a pull request, include a concise, informative
  description of the change being made.


Code Style
----------
Try to mimic the current code style which strives for something like
http://geosoft.no/development/javastyle.html, with the following
exceptions:

8  Fields must be prefixed with `_` and referenced without an explicit
   this.
61 Indentation must be `4` and must consist of only whitespace.
65 `else` clause must be on same line as previous closing bracket.
72 Single line statement `if-else`, `for` or `while` statements must
    always use brackets.
73 `case 100: // NOT case 100 :`
74 Method names can _not_ be followed by a whitespace when it is
   followed by another name.

- No tabs, no carriage returns, no trailing whitespace.
- Lines should be at most 80 characters long.
- Default to immutable fields and strive for possibly immutable
  objects.
- Try to limit unnecessary usage of null. Assert fields that aren't
  allowed to be null aren't assigned null. Any method that return null
  should have its name suffixed with orNull.
- Include javadoc for any unchecked exceptions that may be thrown for
  a method.
