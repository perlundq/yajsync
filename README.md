yajsync
=======

yajsync is a port of [rsync](http://rsync.samba.org) written in Java.

yajsync currently supports a minimal subset of rsync protocol version
30.0.

Currently implemented rsync options:

- Incremental recursion (-r, --recursive)

- Preserve owner (-o, --owner)

- Preserve group (-g, --group)

- Don't map uid/gid values by user/group name (--numeric-ids)

- Preserve permissions (-p, --perms)

- Preserve times (-t, --times)

- Preserve symbolic links (-l, --links)

- Transfer directories (-d, --dirs)

- Delete extraneous files (--delete)

- Don't skip files that match size and time (-I, --ignore-times)

- Read daemon-access password from FILE (--password-file=FILE) or environment variable RSYNC_PASSWORD

- Module file listings

- Archive mode (-a, --archive)

Simulated options:

- Preserve character device files and block device files (--devices)

- Preserve named sockets and named pipes (--specials)

These will currently return an error when trying to actually read device
metadata of a device file or trying to create a device file. The reason for this
is the inability to handle device files in Java. We still want to support these
options in order to be able to support --archive.

yajsync is compliant with at least rsync version 3.0.9.


Features
--------

- rsync Java API

- Platform independent rsync server

- Platform independent rsync client with support for both local and
  remote file transfers

- Native SSL/TLS tunneling

Please be aware though that the API currently is unstable, not
documented and will most probably change in the near future.


Warning
-------

This software is still unstable and there might be data corruption
bugs hiding. So use it only carefully at your own risk.


Contact
-------

If you encounter any problems or if you have any questions or just
want to provide feedback of any type, then please create a new github
[issue](https://github.com/perlundq/yajsync/issues/new) for this.


Example
-------

Start a Server listening on localhost port 14415, with one implicitly
read-only module called Downloads and one readable and writable module
called Uploads:

```
$ cat yajsyncd.conf

# This line and all text after a `#' is a comment. Text within square
# brackets define the name of a new module. A module definition may be
# followed by any number of predefined parameter value statements on
# the form key = value. The current available module parameters are:
#
#    path          An existing path to the module (mandatory).
#    comment       Text that will be shown to the client in module listings
#                  (optional).
#    is_readable   A boolean (true or false) indicating whether files
#                  may be read below this module (optional, default is
#                  true).
#    is_writable   A boolean (true or false) indicating whether files
#                  may be written below this module (optional, default
#                  is false).
#    fs            A Java file system provider (optional).

# This is a module definition for a module called Downloads. path is
# the only mandatory module parameter. This one also provides a
# comment. All modules are implicitly readable but not writable:
[Downloads]
path = /path/to/Downloads/
comment = this text will be printed on module listings, it is optional
# is_readable = true
# is_writable = false

# Uploads is both readable and writable; it does not provide a
# comment:
[Uploads]
path = /path/to/Uploads/
is_writable = true

# Any non-default Java file system provider may be specified using the
# parameter `fs'. Here is an example using the built in Zip file
# system provider which provides transparent access to the contents of
# a zip-file (see also the client option `--fs'):
[zipfs]
fs = jar:file:/path/to/file.zip
path = /
```

Start the server:
```
$ java -Dumask=$(umask) -jar yajsync-app/target/yajsync-app-0.9.0-SNAPSHOT-full.jar server --port=14415 --config=yajsyncd.conf
```

Recursively upload the directory called example to Uploads:
```
java -Dumask=$(umask) -jar yajsync-app/target/yajsync-app-0.9.0-SNAPSHOT-full.jar client --port=14415 -r example localhost::Uploads
```

The same thing using the alternative syntax:
```
java -Dumask=$(umask) -jar yajsync-app/target/yajsync-app-0.9.0-SNAPSHOT-full.jar client -r example rsync://localhost:14415/Uploads
```

And finally the same thing using the original rsync client:
```
rsync --port=14415 -r example localhost::Uploads
```


Note
----

- Recursive transfers always implies incremental recursion.

- Use ```--charset``` for setting common character set (defaults to
  UTF-8). Note that ```--iconv``` is _not_ supported.

- Client local file transfers always uses rsync:s delta transfer
  algorithm, i.e. it does not have an option ```--whole-file```.

- Checksum block size is not computed in the exact same way as
  rsync. It is computed dynamically based on the file size and is
  always an even multiple of 2 and at least 512 bytes long.

- Wild cards are not supported.


Extra features
--------------

- (Receiver) ```--defer-write``` - defer writing into a temporary file
  until the content of the target file needs to be updated.

- Support for custom [Java file system providers](https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileSystem.html) with client option
  ```--fs``` and server module parameter ```fs```.


Build instructions
------------------

Requirements:

- [git](http://git-scm.com)
- [Apache Maven](https://maven.apache.org)
- [OpenJDK 1.7 or later](http://openjdk.java.net/) or [Oracle JDK 7](http://java.oracle.com)

Procedure:

    git clone https://github.com/perlundq/yajsync.git
    cd yajsync
    mvn


Usage
-----

Show client/server help (-h argument):

(Windows):

    java -jar yajsync-app/target/yajsync-app-0.9.0-SNAPSHOT-full.jar client -h
    java -jar yajsync-app/target/yajsync-app-0.9.0-SNAPSHOT-full.jar server -h

(Unix/Linux, we must inject necessary umask information as a property,
assuming Bourne shell compatible syntax)

    java -Dumask=$(umask) -jar yajsync-app/target/yajsync-app-0.9.0-SNAPSHOT-full.jar client -h
    java -Dumask=$(umask) -jar yajsync-app/target/yajsync-app-0.9.0-SNAPSHOT-full.jar server -h

Recommended extra options to the jvm (i.e. must be placed before the
-jar <jar-file> argument):

Turn on assertions:

    -ea

Use a more memory conservative garbage collector:

    -XX:+UseConcMarkSweepGC

Turn on aggressive optimisations:

    -XX:+AggressiveOpts

SSL/TLS is configured externally (see JSSE documentation), but the
following properties are used (options to the JVM):

    -Djavax.net.ssl.keyStore=...
    -Djavax.net.ssl.keyStoreAlias=...
    -Djavax.net.ssl.keyStorePassword=...
    -Djavax.net.ssl.trustStore=...
    -Djavax.net.ssl.trustStorePassword=...

javax.net.debug is useful for debugging SSL/TLS. To see available
values to javax.net.debug:

    -Djavax.net.debug=help

Note: client side authorisation is not yet implemented - requires
changes to server configuration.


License
-------

Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others

Copyright (C) 2013-2016 Per Lundqvist

yajsync is licensed under GNU General Public License version 3 or
later. See the file LICENSE or http://www.gnu.org/licenses/gpl.txt for
the license details.
