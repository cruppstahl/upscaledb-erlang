%%
%% Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
   | enable_transactions
   | enable_crc32.

-type env_open_flag() ::
   undefined
   | read_only
   | enable_fsync
   | disable_mmap
   | cache_unlimited
   | enable_recovery
   | auto_recovery
   | flush_when_committed
   | enable_transactions
   | enable_crc32.

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

