%%
%% This program is free software; you can redistribute it and/or modify it
%% under the terms of the GNU General Public License as published by the
%% Free Software Foundation; either version 2 of the License, or
%% (at your option) any later version.
%%
%% See files COPYING.* for License information.
%%

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

-opaque env() :: term().
-opaque db() :: term().
-opaque txn() :: term().
-opaque cursor() :: term().

-type env_create_flag() ::
   undefined
   | in_memory
   | enable_fsync
   | disable_mmap
   | cache_unlimited
   | enable_recovery
   | enable_transactions.

-type env_open_flag() ::
   undefined
   | read_only
   | enable_fsync
   | disable_mmap
   | cache_unlimited
   | enable_recovery
   | auto_recovery
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

