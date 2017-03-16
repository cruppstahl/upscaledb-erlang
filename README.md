upscaledb-erlang 2.1.12                             Fr 7. Nov 15:00:58 CET 2015

(C) Christoph Rupp, chris@crupp.de; http://www.upscaledb.com

upscaledb-erlang
================

An erlang driver for upscaledb.

=== Introduction ===

This is the official erlang driver for upscaledb. It's a NIFS based
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

    http://files.upscaledb.com/documentation/html/

Also, you will find several samples in `src/ups_tests.erl`. They should
provide a good starting point.

=== Licensing ===

The erlang driver is released under the Apache Public License 2.0, just
like upscaledb. See the file COPYING for more information.

=== Thanks ===

This library is based on and was inspired by Kevin Smith's "hammy"
(https://github.com/kevsmith/hammy), which is no longer maintained but
nevertheless was extremely helpful to me when developing the new Erlang
driver from scratch.

=== Contact ===

Author of upscaledb is
    Christoph Rupp
    Paul-Preuss-Str. 63
    80995 Muenchen/Germany
    email: chris@crupp.de
    web: http://www.upscaledb.com
