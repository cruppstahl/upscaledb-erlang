%% @author Christoph Rupp <chris@crupp.de>
%% @copyright 2017 Christoph Rupp
%% @version 2.2.1
%%
%% @doc upscaledb-erlang is an erlang driver for upscaledb
%% (https://upscaledb.com).
%% @headerfile "../include/ups.hrl"
%% @reference See the <a href="http://www.upscaledb.com">upscaledb web page</a>
%% for more information about upscaledb, and for reference documentation on the
%% native C API.
%%
%%
%% Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
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

-module(ups).
-author("Christoph Rupp <chris@crupp.de>").

-include("include/ups.hrl").

-export([strerror/1,
   env_create/1, env_create/2, env_create/3, env_create/4,
   env_open/1, env_open/2, env_open/3,
   env_create_db/2, env_create_db/3, env_create_db/4,
   env_open_db/2, env_open_db/3, env_open_db/4,
   env_rename_db/3,
   env_erase_db/2,
   db_insert/3, db_insert/4, db_insert/5,
   db_erase/2, db_erase/3,
   db_find/2, db_find/3, db_find/4,
   db_close/1,
   txn_begin/1, txn_begin/2,
   txn_abort/1,
   txn_commit/1,
   cursor_create/1, cursor_create/2,
   cursor_clone/1, 
   cursor_move/2, 
   cursor_overwrite/2, 
   cursor_find/2,
   cursor_insert/3, cursor_insert/4,
   cursor_erase/1, 
   cursor_get_duplicate_count/1,
   cursor_get_record_size/1,
   cursor_close/1,
   env_close/1,
   uqi_select_range/2, uqi_select_range/3, uqi_select_range/4,
   uqi_result_get_row_count/1,
   uqi_result_get_key_type/1,
   uqi_result_get_record_type/1,
   uqi_result_get_key/2,
   uqi_result_get_record/2,
   uqi_result_close/1
   ]).



%% @doc Translates a upscaledb status code to a descriptive error string.
%% This wraps the native ups_strerror function.
-spec strerror(integer()) ->
  string().
strerror(Status) ->
  ups_nifs:strerror(Status).



%% @doc Creates a new Environment. Expects a filename for the new
%% Environment.
%% This wraps the native ups_env_create function.
-spec env_create(string()) ->
  {ok, env()} | {error, atom()}.
env_create(Filename) ->
  env_create_impl(Filename, [], 0, []).

%% @doc Creates a new Environment. Expects a filename and flags for the new
%% Environment. See @type env_create_flags.
%% This wraps the native ups_env_create function.
-spec env_create(string(), [env_create_flag()]) ->
  {ok, env()} | {error, atom()}.
env_create(Filename, Flags) ->
  env_create_impl(Filename, Flags, 0, []).

%% @doc Creates a new Environment. Expects a filename, flags, file access
%% mode (chmod) and a list of additional parameters for the new
%% Environment. See @type env_create_flags.
%% This wraps the native ups_env_create function.
-spec env_create(string(), [env_create_flag()], integer()) ->
  {ok, env()} | {error, atom()}.
env_create(Filename, Flags, Mode) ->
  env_create_impl(Filename, Flags, Mode, []).

%% @doc Creates a new Environment. Expects a filename, flags, file access
%% mode (chmod) and a list of additional parameters for the new
%% Environment. See @type env_create_flags.
%% This wraps the native ups_env_create function.
-spec env_create(string(), [env_create_flag()], integer(),
       [{atom(), integer() | atom()}]) ->
  {ok, env()} | {error, atom()}.
env_create(Filename, Flags, Mode, Parameters) ->
  env_create_impl(Filename, Flags, Mode, Parameters).



%% @doc Opens an existing Environment. Expects a filename.
%% This wraps the native ups_env_open function.
-spec env_open(string()) ->
  {ok, env()} | {error, atom()}.
env_open(Filename) ->
  env_open_impl(Filename, [], []).

%% @doc Opens an existing Environment. Expects a filename and flags
%% See @type env_open_flags.
%% This wraps the native ups_env_open function.
-spec env_open(string(), [env_open_flag()]) ->
  {ok, env()} | {error, atom()}.
env_open(Filename, Flags) ->
  env_open_impl(Filename, Flags, []).

%% @doc Opens an existing Environment. Expects a filename, flags and
%% additional parameters. See @type env_open_flags.
%% This wraps the native ups_env_open function.
-spec env_open(string(), [env_open_flag()],
       [{atom(), integer() | atom()}]) ->
  {ok, env()} | {error, atom()}.
env_open(Filename, Flags, Parameters) ->
  env_open_impl(Filename, Flags, Parameters).




%% @doc Creates a new Database in an Environment. Expects a handle for the
%% Environment and the name of the new Database.
%% This wraps the native ups_env_create_db function.
-spec env_create_db(env(), integer()) ->
  {ok, db()} | {error, atom()}.
env_create_db(Env, Dbname) ->
  env_create_db_impl(Env, Dbname, [], []).

%% @doc Creates a new Database in an Environment. Expects a handle for the
%% Environment, the name and flags of the new Database.
%% See @type env_create_db_flag.
%% This wraps the native ups_env_create_db function.
-spec env_create_db(env(), integer(), [env_create_db_flag()]) ->
  {ok, db()} | {error, atom()}.
env_create_db(Env, Dbname, Flags) ->
  env_create_db_impl(Env, Dbname, Flags, []).

%% @doc Creates a new Database in an Environment. Expects a handle for the
%% Environment, the name, flags and a list of additional parameters of
%% the new Database.
%% See @type env_create_db_flag.
%% This wraps the native ups_env_create_db function.
-spec env_create_db(env(), integer(), [env_create_db_flag()],
       [{atom(), integer() | atom()}]) ->
  {ok, db()} | {error, atom()}.
env_create_db(Env, Dbname, Flags, Parameters) ->
  env_create_db_impl(Env, Dbname, Flags, Parameters).



%% @doc Opens an existing Database in an Environment. Expects a handle for the
%% Environment and the name of the Database.
%% This wraps the native ups_env_open_db function.
-spec env_open_db(env(), integer()) ->
  {ok, db()} | {error, atom()}.
env_open_db(Env, Dbname) ->
  env_open_db_impl(Env, Dbname, [], []).

%% @doc Opens an existing Database in an Environment. Expects a handle for the
%% Environment, the name and flags of the Database.
%% See @type env_open_db_flag.
%% This wraps the native ups_env_open_db function.
-spec env_open_db(env(), integer(), [env_open_db_flag()]) ->
  {ok, db()} | {error, atom()}.
env_open_db(Env, Dbname, Flags) ->
  env_open_db_impl(Env, Dbname, Flags, []).

%% @doc Opens an existing Database in an Environment. Expects a handle for the
%% Environment, the name, flags and a list of additional parameters of
%% the Database.
%% See @type env_open_db_flag.
%% This wraps the native ups_env_open_db function.
-spec env_open_db(env(), integer(), [env_open_db_flag()],
       [{atom(), integer() | atom()}]) ->
  {ok, db()} | {error, atom()}.
env_open_db(Env, Dbname, Flags, Parameters) ->
  env_open_db_impl(Env, Dbname, Flags, Parameters).



%% @doc Renames a Database. Expects a handle for the Environment, the
%% name of the (existing) Database and the new name.
%% This wraps the native ups_env_rename_db function.
-spec env_rename_db(env(), integer(), integer()) ->
  ok | {error, atom()}.
env_rename_db(Env, Oldname, Newname) ->
  ups_nifs:env_rename_db(Env, Oldname, Newname).



%% @doc Deletes a Database. Expects a handle for the Environment and the
%% name of the (existing) Database. Will fail if this Database is open!
%% This wraps the native ups_env_erase_db function.
-spec env_erase_db(env(), integer()) ->
  ok | {error, atom()}.
env_erase_db(Env, Dbname) ->
  ups_nifs:env_erase_db(Env, Dbname).



%% @doc Closes a Database handle.
%% This wraps the native ups_db_close function.
-spec db_close(db()) ->
  ok | {error, atom()}.
db_close(Db) ->
  ups_nifs:db_close(Db).



%% @doc Closes an Environment handle.
%% This wraps the native ups_env_close function.
env_close(Env) ->
  ups_nifs:env_close(Env).



%% @doc Inserts a new Key/Value pair into the Database.
%% This wraps the native ups_db_insert function.
-spec db_insert(db(), binary(), binary()) ->
  ok | {error, atom()}.
db_insert(Db, Key, Value) ->
  db_insert_impl(Db, undefined, Key, Value, []).

%% @doc Inserts a new Key/Value pair into the Database in a Transaction.
%% This wraps the native ups_db_insert function.
-spec db_insert(db(), txn() | undefined, binary(), binary()) ->
  ok | {error, atom()}.
db_insert(Db, Txn, Key, Value) ->
  db_insert_impl(Db, Txn, Key, Value, []).

%% @doc Inserts a new Key/Value pair into the Database. Accepts additional
%% flags for the operation.
%% This wraps the native ups_db_insert function.
-spec db_insert(db(), txn() | undefined, binary(),
                binary(), [db_insert_flag()]) ->
  ok | {error, atom()}.
db_insert(Db, Txn, Key, Value, Flags) ->
  db_insert_impl(Db, Txn, Key, Value, Flags).

%% @doc Erases a Key/Value pair (including all duplicates) from the Database.
%% This wraps the native ups_db_erase function.
-spec db_erase(db(), binary()) ->
  ok | {error, atom()}.
db_erase(Db, Key) ->
  ups_nifs:db_erase(Db, undefined, Key).

%% @doc Erases a Key/Value pair (including all duplicates) from the Database.
%% This wraps the native ups_db_erase function.
-spec db_erase(db(), txn() | undefined, binary()) ->
  ok | {error, atom()}.
db_erase(Db, Txn, Key) ->
  ups_nifs:db_erase(Db, Txn, Key).

%% @doc Lookup of a Key; returns the associated value from the Database.
%% This wraps the native ups_db_find function.
-spec db_find(db(), binary()) ->
  {ok, binary()} | {error, atom()}.
db_find(Db, Key) ->
  ups_nifs:db_find(Db, undefined, Key).

%% @doc Lookup of a Key; returns the associated value from the Database.
%% This wraps the native ups_db_find function.
-spec db_find(db(), txn() | undefined, binary()) ->
  {ok, binary()} | {error, atom()}.
db_find(Db, Txn, Key) ->
  ups_nifs:db_find(Db, Txn, Key).

%% @doc Lookup of a Key; returns the associated value from the Database.
%% This wraps the native ups_db_find function. It returns key AND record!
-spec db_find(db(), txn() | undefined, binary(), [db_find_flag()]) ->
  {ok, binary(), binary()} | {error, atom()}.
db_find(Db, Txn, Key, Flags) ->
  ups_nifs:db_find_flags(Db, Txn, Key, find_db_flags(Flags, 0)).



%% @doc Begins a new Transaction.
%% This wraps the native ups_txn_begin function.
-spec txn_begin(env()) ->
  {ok, binary()} | {error, atom()}.
txn_begin(Env) ->
  ups_nifs:txn_begin(Env, 0).

%% @doc Begins a new Transaction.
%% This wraps the native ups_txn_begin function.
-spec txn_begin(env(), [txn_begin_flag()]) ->
  {ok, binary()} | {error, atom()}.
txn_begin(Env, Flags) ->
  ups_nifs:txn_begin(Env, txn_begin_flags(Flags, 0)).

%% @doc Aborts a running Transaction.
%% This wraps the native ups_txn_abort function.
-spec txn_abort(txn()) ->
  ok | {error, atom()}.
txn_abort(Txn) ->
  ups_nifs:txn_abort(Txn).

%% @doc Commits a running Transaction.
%% This wraps the native ups_txn_commit function.
-spec txn_commit(txn()) ->
  ok | {error, atom()}.
txn_commit(Txn) ->
  ups_nifs:txn_commit(Txn).



%% @doc Creates a new Cursor for traversing a Database.
%% This wraps the native ups_cursor_create function.
-spec cursor_create(db()) ->
  ok | {error, atom()}.
cursor_create(Db) ->
  ups_nifs:cursor_create(Db, undefined).

%% @doc Creates a new Cursor for traversing a Database in a Transaction.
%% This wraps the native ups_cursor_create function.
-spec cursor_create(db(), txn()) ->
  ok | {error, atom()}.
cursor_create(Db, Txn) ->
  ups_nifs:cursor_create(Db, Txn).

%% @doc Clones a Cursor.
%% This wraps the native ups_cursor_clone function.
-spec cursor_clone(cursor()) ->
  ok | {error, atom()}.
cursor_clone(Cursor) ->
  ups_nifs:cursor_clone(Cursor).

%% @doc Moves a Cursor in the direction specified in the flags; returns
%% Key and Record.
%% This wraps the native ups_cursor_move function.
-spec cursor_move(cursor(), [cursor_move_flag()]) ->
  {ok, binary(), binary()} | {error, atom()}.
cursor_move(Cursor, Flags) ->
  ups_nifs:cursor_move(Cursor, cursor_move_flags(Flags, 0)).

%% @doc Overwrites the Record of the Cursor.
%% This wraps the native ups_cursor_overwrite function.
-spec cursor_overwrite(cursor(), binary()) ->
  ok | {error, atom()}.
cursor_overwrite(Cursor, Record) ->
  ups_nifs:cursor_overwrite(Cursor, Record).

%% @doc Performs a lookup and points the Cursor to the found key. Returns
%% the Record. This wraps the native ups_cursor_find function.
-spec cursor_find(cursor(), binary()) ->
  {ok, binary()} | {error, atom()}.
cursor_find(Cursor, Key) ->
  ups_nifs:cursor_find(Cursor, Key).

%% @doc Inserts a Key/Record pair into the Database and points the Cursor
%% to the inserted Key.
%% This wraps the native ups_cursor_insert function.
-spec cursor_insert(cursor(), binary(), binary()) ->
  ok | {error, atom()}.
cursor_insert(Cursor, Key, Record) ->
  ups_nifs:cursor_insert(Cursor, Key, Record, 0).

%% @doc Inserts a Key/Record pair into the Database and points the Cursor
%% to the inserted Key. Supports additional flags.
%% This wraps the native ups_cursor_insert function.
-spec cursor_insert(cursor(), binary(), binary(), [cursor_insert_flag()]) ->
  ok | {error, atom()}.
cursor_insert(Cursor, Key, Record, Flags) ->
  ups_nifs:cursor_insert(Cursor, Key, Record, cursor_insert_flags(Flags, 0)).

%% @doc Erases the current Key/Record pair.
%% This wraps the native ups_cursor_erase function.
-spec cursor_erase(cursor()) ->
  ok | {error, atom()}.
cursor_erase(Cursor) ->
  ups_nifs:cursor_erase(Cursor).

%% @doc Returns the number of duplicate keys.
%% This wraps the native ups_cursor_get_duplicate_count function.
-spec cursor_get_duplicate_count(cursor()) ->
  {ok, integer()} | {error, atom()}.
cursor_get_duplicate_count(Cursor) ->
  ups_nifs:cursor_get_duplicate_count(Cursor).

%% @doc Returns the record size of the current key.
%% This wraps the native ups_cursor_get_record_size function.
-spec cursor_get_record_size(cursor()) ->
  {ok, integer()} | {error, atom()}.
cursor_get_record_size(Cursor) ->
  ups_nifs:cursor_get_record_size(Cursor).

%% @doc Closes the Cursor.
%% This wraps the native ups_cursor_close function.
-spec cursor_close(cursor()) ->
  ok | {error, atom()}.
cursor_close(Cursor) ->
  ups_nifs:cursor_close(Cursor).

%% @doc Performs a range select over a database.
%% This wraps the native uqi_select_range function.
-spec uqi_select_range(env(), string()) ->
  {ok, result()} | {error, atom()}.
uqi_select_range(Env, Query) ->
  ups_nifs:uqi_select_range(Env, Query, undefined, undefined).

%% @doc Performs a range select over a database.
%% This wraps the native uqi_select_range function.
-spec uqi_select_range(env(), string(), cursor()) ->
  {ok, result()} | {error, atom()}.
uqi_select_range(Env, Query, Cursor) ->
  ups_nifs:uqi_select_range(Env, Query, Cursor, undefined).

%% @doc Performs a range select over a database.
%% This wraps the native uqi_select_range function.
-spec uqi_select_range(env(), string(), cursor(), cursor()) ->
  {ok, result()} | {error, atom()}.
uqi_select_range(Env, Query, Cursor1, Cursor2) ->
  ups_nifs:uqi_select_range(Env, Query, Cursor1, Cursor2).

%% @doc Returns the number of rows stored in an UQI result.
%% This wraps the native uqi_result_get_row_count function.
-spec uqi_result_get_row_count(result()) ->
  {ok, integer()} | {error, atom()}.
uqi_result_get_row_count(Result) ->
  ups_nifs:uqi_result_get_row_count(Result).

%% @doc Returns the key type of an UQI result set.
%% This wraps the native uqi_result_get_key_type function.
-spec uqi_result_get_key_type(result()) ->
  {ok, integer()} | {error, atom()}.
uqi_result_get_key_type(Result) ->
  ups_nifs:uqi_result_get_key_type(Result).

%% @doc Returns the record type of an UQI result set.
%% This wraps the native uqi_result_get_record_type function.
-spec uqi_result_get_record_type(result()) ->
  {ok, integer()} | {error, atom()}.
uqi_result_get_record_type(Result) ->
  ups_nifs:uqi_result_get_record_type(Result).

%% @doc Returns a key of an UQI result set.
%% This wraps the native uqi_result_get_key function.
-spec uqi_result_get_key(result(), integer()) ->
  {ok, binary()} | {error, atom()}.
uqi_result_get_key(Result, Row) ->
  ups_nifs:uqi_result_get_key(Result, Row).

%% @doc Returns a record of an UQI result set.
%% This wraps the native uqi_result_get_record function.
-spec uqi_result_get_record(result(), integer()) ->
  {ok, binary()} | {error, atom()}.
uqi_result_get_record(Result, Row) ->
  ups_nifs:uqi_result_get_record(Result, Row).

%% @doc Closes an UQI result set.
%% This wraps the native uqi_result_close function.
-spec uqi_result_close(result()) ->
  ok | {error, atom()}.
uqi_result_close(Result) ->
  ups_nifs:uqi_result_close(Result).

%% Private functions

env_create_impl(Filename, Flags, Mode, Parameters) ->
  ups_nifs:env_create(Filename, env_create_flags(Flags, 0), Mode, Parameters).

env_open_impl(Filename, Flags, Parameters) ->
  ups_nifs:env_open(Filename, env_open_flags(Flags, 0), Parameters).

env_create_db_impl(Env, Dbname, Flags, Parameters) ->
  ups_nifs:env_create_db(Env, Dbname, env_create_db_flags(Flags, 0), Parameters).

env_open_db_impl(Env, Dbname, Flags, Parameters) ->
  ups_nifs:env_open_db(Env, Dbname, env_open_db_flags(Flags, 0), Parameters).

db_insert_impl(Db, Txn, Key, Value, Flags) ->
  ups_nifs:db_insert(Db, Txn, Key, Value, insert_db_flags(Flags, 0)).

env_create_flags([], Acc) ->
  Acc;
env_create_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      env_create_flags(Tail, Acc);
    in_memory ->
      env_create_flags(Tail, Acc bor 16#00080);
    enable_fsync ->
      env_create_flags(Tail, Acc bor 16#00001);
    disable_mmap ->
      env_create_flags(Tail, Acc bor 16#00200);
    cache_unlimited ->
      env_create_flags(Tail, Acc bor 16#40000);
    enable_recovery ->
      env_create_flags(Tail, Acc bor 16#08000);
    flush_when_committed ->
      env_create_flags(Tail, Acc bor 16#1000000);
    enable_transactions ->
      env_create_flags(Tail, Acc bor 16#20000);
    enable_crc32 ->
      env_create_flags(Tail, Acc bor 16#2000000)
  end.

env_open_flags([], Acc) ->
  Acc;
env_open_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      env_open_flags(Tail, Acc);
    read_only ->
      env_open_flags(Tail, Acc bor 16#00004);
    enable_fsync ->
      env_open_flags(Tail, Acc bor 16#00001);
    disable_mmap ->
      env_open_flags(Tail, Acc bor 16#00200);
    cache_unlimited ->
      env_open_flags(Tail, Acc bor 16#40000);
    enable_recovery ->
      env_open_flags(Tail, Acc bor 16#08000);
    auto_recovery ->
      env_open_flags(Tail, Acc bor 16#10000);
    flush_when_committed ->
      env_create_flags(Tail, Acc bor 16#1000000);
    enable_transactions ->
      env_open_flags(Tail, Acc bor 16#20000);
    enable_crc32 ->
      env_create_flags(Tail, Acc bor 16#2000000)
  end.

env_create_db_flags([], Acc) ->
  Acc;
env_create_db_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      env_open_flags(Tail, Acc);
    enable_duplicate_keys ->
      env_open_flags(Tail, Acc bor 16#04000);
    force_records_inline ->
      env_open_flags(Tail, Acc bor 16#800000);
    record_number32 ->
      env_open_flags(Tail, Acc bor 16#01000);
    record_number ->
      env_open_flags(Tail, Acc bor 16#02000);
    record_number64 ->
      env_open_flags(Tail, Acc bor 16#02000)
  end.

env_open_db_flags([], Acc) ->
  Acc;
env_open_db_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      env_open_flags(Tail, Acc);
    read_only ->
      env_open_flags(Tail, Acc bor 16#00004)
  end.

insert_db_flags([], Acc) ->
  Acc;
insert_db_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      insert_db_flags(Tail, Acc);
    overwrite ->
      insert_db_flags(Tail, Acc bor 16#00001);
    duplicate ->
      insert_db_flags(Tail, Acc bor 16#00002)
  end.

find_db_flags([], Acc) ->
  Acc;
find_db_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      find_db_flags(Tail, Acc);
    gt_match ->
      find_db_flags(Tail, Acc bor 16#2000);
    lt_match ->
      find_db_flags(Tail, Acc bor 16#1000);
    geq_match ->
      find_db_flags(Tail, Acc bor 16#6000);
    leq_match ->
      find_db_flags(Tail, Acc bor 16#5000)
  end.

txn_begin_flags([], Acc) ->
  Acc;
txn_begin_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      txn_begin_flags(Tail, Acc);
    temporary ->
      txn_begin_flags(Tail, Acc bor 16#00002);
    read_only ->
      txn_begin_flags(Tail, Acc bor 16#00001)
  end.

cursor_move_flags([], Acc) ->
  Acc;
cursor_move_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      cursor_move_flags(Tail, Acc);
    first ->
      cursor_move_flags(Tail, Acc bor 16#0001);
    last ->
      cursor_move_flags(Tail, Acc bor 16#0002);
    next ->
      cursor_move_flags(Tail, Acc bor 16#0004);
    previous ->
      cursor_move_flags(Tail, Acc bor 16#0008);
    skip_duplicates ->
      cursor_move_flags(Tail, Acc bor 16#0010);
    only_duplicates ->
      cursor_move_flags(Tail, Acc bor 16#0020)
  end.

cursor_insert_flags([], Acc) ->
  Acc;
cursor_insert_flags([Flag | Tail], Acc) ->
  case Flag of
    undefined ->
      cursor_insert_flags(Tail, Acc);
    overwrite ->
      cursor_insert_flags(Tail, Acc bor 16#0001);
    duplicate ->
      cursor_insert_flags(Tail, Acc bor 16#0002);
    duplicate_insert_before ->
      cursor_insert_flags(Tail, Acc bor 16#0004);
    duplicate_insert_after ->
      cursor_insert_flags(Tail, Acc bor 16#0008);
    duplicate_insert_first ->
      cursor_insert_flags(Tail, Acc bor 16#0010);
    duplicate_insert_last ->
      cursor_insert_flags(Tail, Acc bor 16#0020)
  end.
