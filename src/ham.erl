%% @author Christoph Rupp <chris@crupp.de>
%% @copyright 2014 Christoph Rupp
%% @version 2.1.5
%%
%% @doc hamsterdb-erlang is an erlang driver for hamsterdb
%% (http://hamsterdb.com).
%% @reference See the <a href="www.hamsterdb.com">hamsterdb web page</a> for
%% more information about hamsterdb, and for reference documentation on the
%% native C API.

-module(ham).
-author("Christoph Rupp <chris@crupp.de>").

-export([strerror/1,
     env_create/1, env_create/2, env_create/3, env_create/4,
     env_open/1, env_open/2, env_open/3,
     env_create_db/2, env_create_db/3, env_create_db/4,
     env_open_db/2, env_open_db/3, env_open_db/4,
     env_rename_db/3,
     env_erase_db/2,
     db_close/1,
     env_close/1]).

-opaque env() :: term().
-opaque db() :: term().


%% @doc Translates a hamsterdb status code to a descriptive error string.
%% This wraps the native ham_strerror function.
-spec strerror(integer()) ->
  string().
strerror(Status) ->
  strerror_impl(Status).



-type env_create_flag() ::
     undefined
     | in_memory
     | enable_fsync
     | disable_mmap
     | cache_unlimited
     | enable_recovery.

%% @doc Creates a new Environment. Expects a filename for the new
%% Environment.
%% This wraps the native ham_env_create function.
-spec env_create(string()) ->
  {ok, env()} | {error, atom()}.
env_create(Filename) ->
  env_create_impl(Filename, [], 0, []).

%% @doc Creates a new Environment. Expects a filename and flags for the new
%% Environment. See @type env_create_flags.
%% This wraps the native ham_env_create function.
-spec env_create(string(), [env_create_flag()]) ->
  {ok, env()} | {error, atom()}.
env_create(Filename, Flags) ->
  env_create_impl(Filename, Flags, 0, []).

%% @doc Creates a new Environment. Expects a filename, flags, file access
%% mode (chmod) and a list of additional parameters for the new
%% Environment. See @type env_create_flags.
%% This wraps the native ham_env_create function.
-spec env_create(string(), [env_create_flag()], integer()) ->
  {ok, env()} | {error, atom()}.
env_create(Filename, Flags, Mode) ->
  env_create_impl(Filename, Flags, Mode, []).

%% @doc Creates a new Environment. Expects a filename, flags, file access
%% mode (chmod) and a list of additional parameters for the new
%% Environment. See @type env_create_flags.
%% This wraps the native ham_env_create function.
-spec env_create(string(), [env_create_flag()], integer(),
         [{atom(), integer() | atom()}]) ->
  {ok, env()} | {error, atom()}.
env_create(Filename, Flags, Mode, Parameters) ->
  env_create_impl(Filename, Flags, Mode, Parameters).



-type env_open_flag() ::
     undefined
     | read_only
     | enable_fsync
     | disable_mmap
     | cache_unlimited
     | enable_recovery
     | auto_recovery.

%% @doc Opens an existing Environment. Expects a filename.
%% This wraps the native ham_env_open function.
-spec env_open(string()) ->
  {ok, env()} | {error, atom()}.
env_open(Filename) ->
  env_open_impl(Filename, [], []).

%% @doc Opens an existing Environment. Expects a filename and flags
%% See @type env_open_flags.
%% This wraps the native ham_env_open function.
-spec env_open(string(), [env_open_flag()]) ->
  {ok, env()} | {error, atom()}.
env_open(Filename, Flags) ->
  env_open_impl(Filename, Flags, []).

%% @doc Opens an existing Environment. Expects a filename, flags and
%% additional parameters. See @type env_open_flags.
%% This wraps the native ham_env_open function.
-spec env_open(string(), [env_open_flag()],
         [{atom(), integer() | atom()}]) ->
  {ok, env()} | {error, atom()}.
env_open(Filename, Flags, Parameters) ->
  env_open_impl(Filename, Flags, Parameters).




-type env_create_db_flag() ::
     undefined
     | enable_duplicate_keys
     | record_number.

%% @doc Creates a new Database in an Environment. Expects a handle for the
%% Environment and the name of the new Database.
%% This wraps the native ham_env_create_db function.
-spec env_create_db(env(), integer()) ->
  {ok, db()} | {error, atom()}.
env_create_db(Env, Dbname) ->
  env_create_db_impl(Env, Dbname, [], []).

%% @doc Creates a new Database in an Environment. Expects a handle for the
%% Environment, the name and flags of the new Database.
%% See @type env_create_db_flag.
%% This wraps the native ham_env_create_db function.
-spec env_create_db(env(), integer(), [env_create_db_flag()]) ->
  {ok, db()} | {error, atom()}.
env_create_db(Env, Dbname, Flags) ->
  env_create_db_impl(Env, Dbname, Flags, []).

%% @doc Creates a new Database in an Environment. Expects a handle for the
%% Environment, the name, flags and a list of additional parameters of
%% the new Database.
%% See @type env_create_db_flag.
%% This wraps the native ham_env_create_db function.
-spec env_create_db(env(), integer(), [env_create_db_flag()],
         [{atom(), integer() | atom()}]) ->
  {ok, db()} | {error, atom()}.
env_create_db(Env, Dbname, Flags, Parameters) ->
  env_create_db_impl(Env, Dbname, Flags, Parameters).



-type env_open_db_flag() ::
     undefined
     | read_only.

%% @doc Opens an existing Database in an Environment. Expects a handle for the
%% Environment and the name of the Database.
%% This wraps the native ham_env_open_db function.
-spec env_open_db(env(), integer()) ->
  {ok, db()} | {error, atom()}.
env_open_db(Env, Dbname) ->
  env_open_db_impl(Env, Dbname, [], []).

%% @doc Opens an existing Database in an Environment. Expects a handle for the
%% Environment, the name and flags of the Database.
%% See @type env_open_db_flag.
%% This wraps the native ham_env_open_db function.
-spec env_open_db(env(), integer(), [env_open_db_flag()]) ->
  {ok, db()} | {error, atom()}.
env_open_db(Env, Dbname, Flags) ->
  env_open_db_impl(Env, Dbname, Flags, []).

%% @doc Opens an existing Database in an Environment. Expects a handle for the
%% Environment, the name, flags and a list of additional parameters of
%% the Database.
%% See @type env_open_db_flag.
%% This wraps the native ham_env_open_db function.
-spec env_open_db(env(), integer(), [env_open_db_flag()],
         [{atom(), integer() | atom()}]) ->
  {ok, db()} | {error, atom()}.
env_open_db(Env, Dbname, Flags, Parameters) ->
  env_open_db_impl(Env, Dbname, Flags, Parameters).



%% @doc Renames a Database. Expects a handle for the Environment, the
%% name of the (existing) Database and the new name.
%% This wraps the native ham_env_rename_db function.
-spec env_rename_db(env(), integer(), integer()) ->
  ok | {error, atom()}.
env_rename_db(Env, Oldname, Newname) ->
  env_rename_db_impl(Env, Oldname, Newname).



%% @doc Deletes a Database. Expects a handle for the Environment and the
%% name of the (existing) Database. Will fail if this Database is open!
%% This wraps the native ham_env_erase_db function.
-spec env_erase_db(env(), integer()) ->
  ok | {error, atom()}.
env_erase_db(Env, Dbname) ->
  env_erase_db_impl(Env, Dbname).


%% @doc Closes a Database handle.
%% This wraps the native ham_db_close function.
-spec db_close(db()) ->
  ok | {error, atom()}.
db_close(Db) ->
  db_close_impl(Db).


%% @doc Closes an Environment handle.
%% This wraps the native ham_env_close function.
env_close(Env) ->
  env_close_impl(Env).

%% Private functions

strerror_impl(Status) ->
  ham_nifs:strerror(Status).

env_create_impl(Filename, Flags, Mode, Parameters) ->
  ham_nifs:env_create(Filename, env_create_flags(Flags, 0), Mode, Parameters).

env_open_impl(Filename, Flags, Parameters) ->
  ham_nifs:env_open(Filename, env_open_flags(Flags, 0), Parameters).

env_create_db_impl(Env, Dbname, Flags, Parameters) ->
  ham_nifs:env_create_db(Env, Dbname, env_create_db_flags(Flags, 0), Parameters).

env_open_db_impl(Env, Dbname, Flags, Parameters) ->
  ham_nifs:env_open_db(Env, Dbname, env_open_db_flags(Flags, 0), Parameters).

env_rename_db_impl(Env, Oldname, Newname) ->
  ham_nifs:env_rename_db(Env, Oldname, Newname).

env_erase_db_impl(Env, Dbname) ->
  ham_nifs:env_erase_db(Env, Dbname).

db_close_impl(Db) ->
  ham_nifs:db_close(Db).

env_close_impl(Env) ->
  ham_nifs:env_close(Env).

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
        env_create_flags(Tail, Acc bor 16#08000)
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
        env_open_flags(Tail, Acc bor 16#10000)
    end.

env_create_db_flags([], Acc) ->
    Acc;
env_create_db_flags([Flag | Tail], Acc) ->
    case Flag of
      undefined ->
        env_open_flags(Tail, Acc);
      enable_duplicate_keys ->
        env_open_flags(Tail, Acc bor 16#04000);
      record_number ->
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
