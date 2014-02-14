hamsterdb-erlang 2.1.4                            Sun Aug 11 21:20:54 CEST 2013
(C) Christoph Rupp, chris@crupp.de; http://www.hamsterdb.com

hamsterdb-erlang
================

An erlang driver for hamsterdb.

This is work in progress. Will be ready soon.
================

== Introduction

This is the official erlang driver for hamsterdb. It's a NIFS based
implementation including samples and eqc (Erlang QuickCheck) tests.

== Installation

First start the build by running

    make

If you have a commercial QuickCheck license, you can run the QuickCheck
tests:

    ./rebar eqc

or

    make test

For documentation, simply run 

    ./rebar doc

== Usage

After building the documentation, open "doc/index.html" in a browser. It
contains basic information about the module reference. Since the erlang API is
modelled *very* closely after the native API, I recommend using the native
API's documentation whenever the erlang documentation is not sufficient. You
can find it here:

    http://hamsterdb.com/scripts/html_www/index.html

Also, you will find samples in the "samples" directory. They should provide a
good starting point.

== Licensing

The erlang driver is released under GPL2 or later, just like hamsterdb.
hamsterdb Embedded Storage (including the erlang driver) can be licensed
on a per-developer basis for closed source applications. For more details, see
http://hamsterdb.com/licensing and http://hamsterdb.com/store.

There are several exceptions for other open source licenses (see
http://hamsterdb.com/licensing/exceptions for legal information and a license
list).

== Contact

Author of hamsterdb Embedded Storage is
    Christoph Rupp
    Paul-Preuss-Str. 63
    80995 Muenchen/Germany
    email: chris@crupp.de
    web: http://www.hamsterdb.com
