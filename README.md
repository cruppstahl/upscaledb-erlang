hamsterdb-erlang 0.0.1                            So 16. Feb 21:49:05 CET 2014

(C) Christoph Rupp, chris@crupp.de; http://www.hamsterdb.com

hamsterdb-erlang
================

An erlang driver for hamsterdb.

=== Introduction ===

This is the official erlang driver for hamsterdb. It's a NIFS based
implementation including samples and eqc (Erlang QuickCheck) tests.

=== Installation ===

First start the build by running

    make

If you have a commercial QuickCheck license, you can run the QuickCheck
tests:

    ./rebar eqc

or

    make test

For documentation, simply run 

    ./rebar doc

=== Usage ===

After building the documentation, open "doc/index.html" in a browser. It
contains basic information about the module reference. Since the erlang API is
modelled *very* closely after the native API, I recommend using the native
API's documentation whenever the erlang documentation is not sufficient. You
can find it here:

    http://hamsterdb.com/scripts/html_www/index.html

Also, you will find several samples in `src/ham_tests.erl`. They should
provide a good starting point.

=== Licensing ===

The erlang driver is released under the Apache Public License (APL) 2.0, just
like hamsterdb. The APL allows unrestricted use for commercial and
non-commercial applications. See the file COPYING for more information.

A commercial, closed source version hamsterdb pro with additional functionality
is available on request. See http://hamsterdb.com for more information.

=== Thanks ===

This library is based on and was inspired by Kevin Smith's "hammy"
(https://github.com/kevsmith/hammy), which is no longer maintained but
nevertheless was extremely helpful for me when developing the new Erlang
driver from scratch.

=== Contact ===

Author of hamsterdb Embedded Storage is
    Christoph Rupp
    Paul-Preuss-Str. 63
    80995 Muenchen/Germany
    email: chris@crupp.de
    web: http://www.hamsterdb.com
