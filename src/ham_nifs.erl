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

-module(ham_nifs).
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
     cursor_close/1]).

-define(MISSING_NIF, {error, missing_nif}).
-define(NIF_API_VERSION, 1).


init() ->
  Priv = case code:priv_dir(ham) of
    {error, bad_name} ->
      D = filename:dirname(code:which(?MODULE)),
      filename:join([D, "..", "priv"]);
    Dir ->
      Dir
  end,
  SoName = filename:join([Priv, "ham_nifs"]),
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

