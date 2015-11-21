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

-module(ups_eqc1).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-define(UPS_TYPE_BINARY, 0).
-define(UPS_TYPE_CUSTOM, 1).
-define(UPS_TYPE_UINT8, 3).
-define(UPS_TYPE_UINT16, 5).
-define(UPS_TYPE_UINT32, 7).
-define(UPS_TYPE_UINT64, 9).
-define(UPS_TYPE_REAL32, 11).
-define(UPS_TYPE_REAL64, 12).
-define(UPS_KEY_SIZE_UNLIMITED, 16#ffff).
-define(UPS_RECORD_SIZE_UNLIMITED, 16#ffffffff).

-compile(export_all).

-record(state, {env_flags = [], % environment flags for ups_env_create
                env_parameters = [], % environment parameters for ups_env_create
                open = [],      % list of open databases [{name, handle}]
                databases = []  % list of existing databases [name]
                }).

run() ->
  eqc:module(?MODULE).

dbname() ->
  choose(1, 10).

initial_state() ->
  #state{}.

% ups_env_create_db ---------------------------------

create_db_flags() ->
  [oneof([undefined, enable_duplicate_keys])].

create_db_parameters() ->
  [{record_size, record_size_gen()}] ++
      frequency([
        {1, [{key_type, ?UPS_TYPE_BINARY}, {key_size, key_size_gen()}]},
        {2, [{key_type, oneof([?UPS_TYPE_UINT8, ?UPS_TYPE_UINT16,
                                 ?UPS_TYPE_UINT32, ?UPS_TYPE_UINT64,
                                 ?UPS_TYPE_REAL32, ?UPS_TYPE_REAL64])}]}]).

record_size_gen() ->
  oneof([choose(0, 32),
         ?SHRINK(?UPS_RECORD_SIZE_UNLIMITED,
                 [choose(0, ?UPS_RECORD_SIZE_UNLIMITED)])]).

key_size_gen() ->
  oneof([choose(1, 128),
         ?SHRINK(?UPS_KEY_SIZE_UNLIMITED,
                 [choose(1,?UPS_KEY_SIZE_UNLIMITED)])]).

create_db_pre(State) ->
  length(State#state.databases) < 30.

create_db_command(_State) ->
  {call, ?MODULE, create_db, [{var, env}, dbname(), create_db_flags(),
                              create_db_parameters()]}.

create_db(EnvHandle, DbName, DbFlags, DbParams) ->
  case ups:env_create_db(EnvHandle, DbName, DbFlags, lists:flatten(DbParams)) of
    {ok, DbHandle} ->
      DbHandle;
    {error, What} ->
      {error, What}
  end.

create_db_post(State, [_EnvHandle, DbName, _DbFlags, _DbParams], Result) ->
  case lists:member(DbName, State#state.databases) of
    true ->
      eq(Result, {error, database_already_exists});
    false ->
      true
  end.

create_db_next(State, Result, [_EnvHandle, DbName, _DbFlags, _DbParams]) ->
  case lists:member(DbName, State#state.databases) of
    true ->
      State;
    false ->
      State#state{databases = State#state.databases ++ [DbName],
                  open = State#state.open ++ [{DbName, Result}]}
  end.

% ups_env_open_db ---------------------------------

open_db_flags() ->
    [elements([undefined, read_only])].

open_db_pre(State) ->
  State#state.open /= [].

open_db_command(_State) ->
  {call, ?MODULE, open_db, [{var, env}, dbname(), open_db_flags()]}.

open_db(EnvHandle, DbName, DbFlags) ->
  case ups:env_open_db(EnvHandle, DbName, DbFlags) of
    {ok, DbHandle} ->
      DbHandle;
    {error, What} ->
      {error, What}
  end.

open_db_post(State, [_EnvHandle, DbName, _DbFlags], Result) ->
  case Result of
    {error, database_already_open} ->
      eq(lists:keymember(DbName, 1, State#state.open), true);
    {error, database_not_found} ->
      eq(lists:member(DbName, State#state.databases), false);
    {error, _} ->
      false;
    _Else ->
      true
  end.

open_db_next(State, Result, [_EnvHandle, DbName, _DbFlags]) ->
  case (lists:keymember(DbName, 1, State#state.open)) of
    true ->
      State;
    false ->
      case (lists:member(DbName, State#state.databases)) of
        true ->
          State#state{open = State#state.open ++ [{DbName, Result}]};
        false ->
          State
      end
  end.

% ups_env_erase_db ---------------------------------

erase_db_pre(State) ->
  State#state.databases /= [].

erase_db_pre(State, [_EnvHandle, DbName]) ->
  lists:keymember(DbName, 1, State#state.open) == false
    andalso lists:member(DbName, State#state.databases) == true.

erase_db_command(_State) ->
  {call, ?MODULE, erase_db, [{var, env}, dbname()]}.

erase_db(EnvHandle, DbName) ->
  ups:env_erase_db(EnvHandle, DbName).

erase_db_post(_State, [_EnvHandle, _DbName], Result) ->
  Result == ok.

erase_db_next(State, _Result, [_EnvHandle, DbName]) ->
  State#state{databases = lists:delete(DbName, State#state.databases)}.

% ups_env_rename_db ---------------------------------

rename_db_pre(State) ->
  State#state.databases /= [].

rename_db_pre(State, [_EnvHandle, OldName, NewName]) ->
  % names must not be identical
  OldName /= NewName
    % database must not be open
    andalso lists:keymember(OldName, 1, State#state.open) == false
    % database must exist
    andalso lists:keymember(OldName, 1, State#state.databases) == true
    % new name must not exist
    andalso lists:member(NewName, State#state.databases) == false.

rename_db_command(_State) ->
  {call, ?MODULE, rename_db, [{var, env}, dbname(), dbname()]}.

rename_db(EnvHandle, OldName, NewName) ->
  ups:env_rename_db(EnvHandle, OldName, NewName).

rename_db_post(_State, [_EnvHandle, _OldName, _NewName], Result) ->
  Result == ok.

rename_db_next(State, _Result, [_EnvHandle, OldName, NewName]) ->
  State#state{databases
              = lists:delete(OldName, State#state.databases ++ [NewName])}.

% ups_env_close_db ---------------------------------

dbhandle(State) ->
  elements(State#state.open).

db_close_pre(State) ->
  State#state.open /= [].

db_close_command(State) ->
  {call, ?MODULE, db_close, [dbhandle(State)]}.

db_close({_DbName, DbHandle}) ->
  % silently ignore error messages
  case DbHandle of
    {error, _} ->
      ok;
    _ ->
      ups:db_close(DbHandle)
  end.

db_close_post(_State, [{_DbName, _DbHandle}], Result) ->
  Result == ok.

db_close_next(State, _Result, [{DbName, _DbHandle}]) ->
  State#state{open = lists:keydelete(DbName, 1, State#state.open)}.

env_flags() ->
  list(oneof([undefined, enable_recovery])).

env_parameters() ->
  list(elements([{cache_size, 1000000}])).

prop_ups1() ->
  ?FORALL({EnvFlags, EnvParams}, {env_flags(), env_parameters()},
    ?FORALL(Cmds, more_commands(100,
              commands(?MODULE, #state{env_flags = EnvFlags,
                              env_parameters = EnvParams})),
      begin
        %io:format("flags ~p, params ~p ~n", [EnvFlags, EnvParams]),
        {ok, EnvHandle} = ups:env_create("ups_eqc.db", EnvFlags, 0,
                                       EnvParams),
        {History, State, Result} = run_commands(?MODULE, Cmds,
                                            [{env, EnvHandle}]),
        eqc_statem:show_states(
          pretty_commands(?MODULE, Cmds, {History, State, Result},
            aggregate(command_names(Cmds),
              %%collect(length(Cmds),
                begin
                  ups:env_close(EnvHandle),
                  Result == ok
                end)))%%)
      end)).

