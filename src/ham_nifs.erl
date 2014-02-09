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
     db_insert/4,
     db_erase/2,
     db_find/2,
     db_close/1,
     env_close/1]).

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

db_insert(_Db, _Key, _Value, _Flags) ->
  throw(?MISSING_NIF).

db_erase(_Db, _Key) ->
  throw(?MISSING_NIF).

db_find(_Db, _Key) ->
  throw(?MISSING_NIF).

db_close(_Db) ->
  throw(?MISSING_NIF).

env_close(_Env) ->
  throw(?MISSING_NIF).

