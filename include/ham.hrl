%%
%% Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to
%% deal in the Software without restriction, including without limitation the
%% rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
%% sell copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
%% DEALINGS IN THE SOFTWARE.
%%

-export_type([env/0, db/0, txn/0, cursor/0]).

-define(HAM_TYPE_BINARY, 0).
-define(HAM_TYPE_CUSTOM, 1).
-define(HAM_TYPE_UINT8, 3).
-define(HAM_TYPE_UINT16, 5).
-define(HAM_TYPE_UINT32, 7).
-define(HAM_TYPE_UINT64, 9).
-define(HAM_TYPE_REAL32, 11).
-define(HAM_TYPE_REAL64, 12).
-define(HAM_KEY_SIZE_UNLIMITED, 16#ffff).
-define(HAM_RECORD_SIZE_UNLIMITED, 16#ffffffff).

-type env() :: term().
-type db() :: term().
-type txn() :: term().
-type cursor() :: term().

-type env_create_flag() ::
   undefined
   | in_memory
   | enable_fsync
   | disable_mmap
   | cache_unlimited
   | enable_recovery
   | flush_when_committed
   | enable_transactions.

-type env_open_flag() ::
   undefined
   | read_only
   | enable_fsync
   | disable_mmap
   | cache_unlimited
   | enable_recovery
   | auto_recovery
   | flush_when_committed
   | enable_transactions.

-type env_create_db_flag() ::
   undefined
   | enable_duplicate_keys
   | record_number.

-type env_open_db_flag() ::
   undefined
   | read_only.

-type db_insert_flag() ::
   undefined
   | overwrite
   | duplicate.

-type txn_begin_flag() ::
   undefined
   | temporary
   | read_only.

-type cursor_move_flag() ::
   undefined
   | first
   | last
   | next
   | previous
   | skip_duplicates
   | only_duplicates.

-type cursor_insert_flag() ::
   undefined
   | overwrite
   | duplicate
   | duplicate_insert_before
   | duplicate_insert_after
   | duplicate_insert_first
   | duplicate_insert_last.

