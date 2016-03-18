%%
%% Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
%%
%% This program is free software: you can redistribute it and/or modify
%% it under the terms of the GNU General Public License as published by
%% the Free Software Foundation, either version 3 of the License, or
%% (at your option) any later version.
%%
%% This program is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%% GNU General Public License for more details.
%%
%% You should have received a copy of the GNU General Public License
%% along with this program.  If not, see <http://www.gnu.org/licenses/>.

-module(ups_nifs).
-author("Christoph Rupp <chris@crupp.de>").

-on_load(init/0).
-export([init/0,
     strerror/1,
     env_create/4,
     env_open/3,
     env_create_db/4,
     env_open_db/4,
     env_rename_db/3,
     env_erase_db/2,
     db_insert/5,
     db_erase/3,
     db_find/3,
     db_find_flags/4,
     db_close/1,
     txn_begin/2,
     txn_abort/1,
     txn_commit/1,
     env_close/1,
     cursor_create/2,
     cursor_clone/1, 
     cursor_move/2, 
     cursor_overwrite/2, 
     cursor_find/2,
     cursor_insert/4,
     cursor_erase/1, 
     cursor_get_duplicate_count/1,
     cursor_get_record_size/1,
     cursor_close/1,
     uqi_select_range/4,
     uqi_result_get_row_count/1,
     uqi_result_get_key_type/1,
     uqi_result_get_record_type/1,
     uqi_result_get_key/2,
     uqi_result_get_record/2,
     uqi_result_close/1
    ]).

-define(MISSING_NIF, {error, missing_nif}).
-define(NIF_API_VERSION, 1).


init() ->
  Priv = case code:priv_dir(ups) of
    {error, bad_name} ->
      D = filename:dirname(code:which(?MODULE)),
      filename:join([D, "..", "priv"]);
    Dir ->
      Dir
  end,
  SoName = filename:join([Priv, "ups_nifs"]),
  erlang:load_nif(SoName, ?NIF_API_VERSION).

strerror(_Status) ->
  throw(?MISSING_NIF).

env_create(_Filename, _Flags, _Mode, _Parameters) ->
  throw(?MISSING_NIF).

env_open(_Filename, _Flags, _Parameters) ->
  throw(?MISSING_NIF).

env_create_db(_Env, _Dbname, _Flags, _Parameters) ->
  throw(?MISSING_NIF).

env_open_db(_Env, _Dbname, _Flags, _Parameters) ->
  throw(?MISSING_NIF).

env_rename_db(_Env, _Oldname, _Newname) ->
  throw(?MISSING_NIF).

env_erase_db(_Env, _Dbname) ->
  throw(?MISSING_NIF).

db_insert(_Db, _Txn, _Key, _Value, _Flags) ->
  throw(?MISSING_NIF).

db_erase(_Db, _Txn, _Key) ->
  throw(?MISSING_NIF).

db_find(_Db, _Txn, _Key) ->
  throw(?MISSING_NIF).

db_find_flags(_Db, _Txn, _Key, _Flags) ->
  throw(?MISSING_NIF).

db_close(_Db) ->
  throw(?MISSING_NIF).

txn_begin(_Env, _Flags) ->
  throw(?MISSING_NIF).

txn_commit(_Txn) ->
  throw(?MISSING_NIF).

txn_abort(_Txn) ->
  throw(?MISSING_NIF).

env_close(_Env) ->
  throw(?MISSING_NIF).

cursor_create(_Env, _Txn) ->
  throw(?MISSING_NIF).

cursor_clone(_Cursor) ->
  throw(?MISSING_NIF).

cursor_move(_Cursor, _Flags) ->
  throw(?MISSING_NIF).

cursor_overwrite(_Cursor, _Record) ->
  throw(?MISSING_NIF).

cursor_find(_Cursor, _Key) ->
  throw(?MISSING_NIF).

cursor_insert(_Cursor, _Key, _Record, _Flags) ->
  throw(?MISSING_NIF).

cursor_erase(_Cursor) ->
  throw(?MISSING_NIF).

cursor_get_duplicate_count(_Cursor) ->
  throw(?MISSING_NIF).

cursor_get_record_size(_Cursor) ->
  throw(?MISSING_NIF).

cursor_close(_Cursor) ->
  throw(?MISSING_NIF).

uqi_select_range(_Env, _Query, _Cursor1, _Cursor2) ->
  throw(?MISSING_NIF).

uqi_result_get_row_count(_Result) ->
  throw(?MISSING_NIF).

uqi_result_get_key_type(_Result) ->
  throw(?MISSING_NIF).

uqi_result_get_record_type(_Result) ->
  throw(?MISSING_NIF).

uqi_result_get_key(_Result, _Row) ->
  throw(?MISSING_NIF).

uqi_result_get_record(_Result, _Row) ->
  throw(?MISSING_NIF).

uqi_result_close(_Result) ->
  throw(?MISSING_NIF).

