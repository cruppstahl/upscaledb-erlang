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

-module(ups_eqc2).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include("include/ups.hrl").

-compile(export_all).

-record(dbstate, {handle, % database handle
                flags = [], % database flags for ups_env_create_db
                parameters = [], % database parameters for ups_env_create_db,
                data = [] % ordset with the data
                }).

-record(state, {env_flags = [], % environment flags for ups_env_create
                open_dbs = [], % list of open databases [{name, #dbstate}]
                closed_dbs = [] % list of closed databases [{name, #dbstate}]
                }).

run() ->
  eqc:module(?MODULE).


dbname() ->
  choose(1, 5).

dbhandle(State) ->
  elements(State#state.open_dbs).

initial_state() ->
  #state{}.

% ups_env_create_db ---------------------------------

% ignore record_number - it has the same characteristics as UPS_TYPE_UINT64,
% but adds lots of complexity to the test
create_db_flags() ->
  [elements([undefined, enable_duplicate_keys])].

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

create_db_pre(State, [_EnvHandle, DbName, _DbFlags, _DbParameters]) ->
  lists:keymember(DbName, 1, State#state.open_dbs) == false
    andalso lists:keymember(DbName, 1, State#state.closed_dbs) == false.

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

create_db_post(_State, [_EnvHandle, _DbName, _DbFlags, _DbParams], Result) ->
  case Result of
    {error, _What} ->
      false;
    _ ->
      true
  end.

create_db_next(State, Result, [_EnvHandle, DbName, DbFlags, DbParams]) ->
  DbState = #dbstate{handle = Result,
                    flags = DbFlags,
                    parameters = DbParams,
                    data = orddict:new()},
  State#state{open_dbs = State#state.open_dbs ++ [{DbName, DbState}]}.

% ups_env_open_db -----------------------------------
open_db_pre(State, [_EnvHandle, DbName]) ->
  lists:keymember(DbName, 1, State#state.open_dbs) == false
    andalso lists:keymember(DbName, 1, State#state.closed_dbs) == true.

open_db_command(_State) ->
  {call, ?MODULE, open_db, [{var, env}, dbname()]}.

open_db(EnvHandle, DbName) ->
  case ups:env_open_db(EnvHandle, DbName) of
    {ok, DbHandle} ->
      DbHandle;
    {error, What} ->
      {error, What}
  end.

open_db_post(_State, [_EnvHandle, _DbName], Result) ->
  case Result of
    {error, _What} ->
      false;
    _ ->
      true
  end.

open_db_next(State, Result, [_EnvHandle, DbName]) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.closed_dbs),
  DbState2 = DbState#dbstate{handle = Result},
  State#state{closed_dbs = lists:keydelete(DbName, 1, State#state.closed_dbs),
        open_dbs = State#state.open_dbs ++ [{DbName, DbState2}]}.

% ups_env_erase_db ---------------------------------
erase_db_pre(State, [_EnvHandle, DbName]) ->
  lists:keymember(DbName, 1, State#state.open_dbs) == false
    andalso lists:keymember(DbName, 1, State#state.closed_dbs) == true.

erase_db_command(_State) ->
  {call, ?MODULE, erase_db, [{var, env}, dbname()]}.

erase_db(EnvHandle, DbName) ->
  ups:env_erase_db(EnvHandle, DbName).

erase_db_post(_State, [_EnvHandle, _DbName], Result) ->
  Result == ok.

erase_db_next(State, _Result, [_EnvHandle, DbName]) ->
  State#state{open_dbs = lists:keydelete(DbName, 1, State#state.open_dbs),
        closed_dbs = lists:keydelete(DbName, 1, State#state.closed_dbs)}.

% ups_db_insert ------------------------------------
key(DbState) ->
  case lists:keyfind(key_type, 1, DbState#dbstate.parameters) of 
    {key_type, ?UPS_TYPE_UINT8} ->
      binary(1);
    {key_type, ?UPS_TYPE_UINT16} ->
      binary(2);
    {key_type, ?UPS_TYPE_UINT32} ->
      binary(4);
    {key_type, ?UPS_TYPE_UINT64} ->
      binary(8);
    {key_type, ?UPS_TYPE_REAL32} ->
      binary(4);
    {key_type, ?UPS_TYPE_REAL64} ->
      binary(8);
    _ ->
      case lists:keyfind(key_size, 1, DbState#dbstate.parameters) of 
        {key_size, ?UPS_KEY_SIZE_UNLIMITED} ->
          binary();
        {key_size, N} ->
          binary(N);
        _ ->
          binary()
      end
  end.

record(DbState) ->
  case lists:keyfind(record_size, 1, DbState#dbstate.parameters) of 
    {record_size, ?UPS_RECORD_SIZE_UNLIMITED} ->
        binary();
    {record_size, N} ->
        binary(N)
  end.

flags(DbState) ->
  case lists:member(enable_duplicate_keys, DbState#dbstate.flags) of
    false ->
      oneof([undefined, overwrite]);
    % duplicates are currently disabled; they are not correctly tracked
    % in the state
    true ->
        oneof([undefined, overwrite]) % , duplicate])
  end.

insert_params(State) ->
  ?LET(Db, elements(State#state.open_dbs),
    begin
      {DbName, DbState} = Db,
      {{DbName, DbState#dbstate.handle}, [flags(DbState)],
                key(DbState), record(DbState)}
    end).

db_insert_pre(State) ->
  length(State#state.open_dbs) > 0.

db_insert_command(State) ->
  {call, ?MODULE, db_insert, [insert_params(State)]}.

db_insert({{_DbName, DbHandle}, Flags, Key, Record}) ->
  ups:db_insert(DbHandle, undefined, Key, Record, Flags).

db_insert_post(State, [{{DbName, _DbHandle}, Flags, Key, _Record}], Result) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case orddict:find(Key, DbState#dbstate.data) of
    {ok, _} ->
      case (lists:member(duplicate, Flags)
            orelse lists:member(overwrite, Flags))of
        true ->
          eq(Result, ok);
        false ->
          eq(Result, {error, duplicate_key})
      end;
    error ->
      eq(Result, ok)
  end.

db_insert_next(State, Result, [{{DbName, _DbHandle}, _Flags, Key, Record}]) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case Result of
    ok ->
      DbState2 = DbState#dbstate{data
                    = orddict:store(Key, Record, DbState#dbstate.data)},
      Databases = lists:keydelete(DbName, 1, State#state.open_dbs),
      State#state{open_dbs = Databases ++ [{DbName, DbState2}]};
    _ ->
      State
  end.

% ups_db_erase -------------------------------------
erase_params(State) ->
  ?LET(Db, elements(State#state.open_dbs),
    begin
      {DbName, DbState} = Db,
      {{DbName, DbState#dbstate.handle}, key(DbState)}
    end).

db_erase_pre(State) ->
  length(State#state.open_dbs) > 0.

db_erase_command(State) ->
  {call, ?MODULE, db_erase, [erase_params(State)]}.

db_erase({{_DbName, DbHandle}, Key}) ->
  ups:db_erase(DbHandle, Key).

db_erase_post(State, [{{DbName, _DbHandle}, Key}], Result) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case orddict:find(Key, DbState#dbstate.data) of
    {ok, _} ->
      eq(Result, ok);
    error ->
      eq(Result, {error, key_not_found})
  end.

db_erase_next(State, Result, [{{DbName, _DbHandle}, Key}]) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case Result of
    ok ->
      DbState2 = DbState#dbstate{data
                    = orddict:erase(Key, DbState#dbstate.data)},
      Databases = lists:keydelete(DbName, 1, State#state.open_dbs),
      State#state{open_dbs = Databases ++ [{DbName, DbState2}]};
    _ ->
      State
  end.

% ups_db_find --------------------------------------
find_params(State) ->
  ?LET(Db, elements(State#state.open_dbs),
    begin
      {DbName, DbState} = Db,
      {{DbName, DbState#dbstate.handle}, key(DbState)}
    end).

db_find_pre(State) ->
  length(State#state.open_dbs) > 0.

db_find_command(State) ->
  {call, ?MODULE, db_find, [find_params(State)]}.

db_find({{_DbName, DbHandle}, Key}) ->
  ups:db_find(DbHandle, Key).

db_find_post(State, [{{DbName, _DbHandle}, Key}], Result) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case orddict:find(Key, DbState#dbstate.data) of
    {ok, Record} ->
      eq(Result, {ok, Record});
    error ->
      eq(Result, {error, key_not_found})
  end.

db_find_next(State, _, _) ->
  State.

% ups_env_close_db ---------------------------------
db_close_pre(State) ->
  length(State#state.open_dbs) > 0.

db_close_command(State) ->
  {call, ?MODULE, db_close, [dbhandle(State)]}.

db_close({_DbName, DbState}) ->
  ups:db_close(DbState#dbstate.handle).

db_close_post(_State, [_Database], Result) ->
  Result == ok.

db_close_next(State, _Result, [{DbName, _DbState}]) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case lists:member(in_memory, State#state.env_flags) of
    true -> % in-memory: remove database
      State#state{
              open_dbs = lists:keydelete(DbName, 1, State#state.open_dbs),
              closed_dbs = lists:keydelete(DbName, 1, State#state.closed_dbs)};
    false -> % disk-based: mark as "closed"
      State#state{
              open_dbs = lists:keydelete(DbName, 1, State#state.open_dbs),
              closed_dbs = State#state.closed_dbs ++ [{DbName, DbState}]}
  end.

env_flags() ->
  oneof([
    [in_memory],
    [cache_unlimited]
  ]).

weight(_State, db_insert) ->
  100;
weight(_State, _) ->
  10.

prop_ups2() ->
  ?FORALL(EnvFlags, env_flags(),
    ?FORALL(Cmds, more_commands(500,
              commands(?MODULE, #state{env_flags = EnvFlags})),
      begin
        {ok, EnvHandle} = ups:env_create("ups_eqc.db", EnvFlags),
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

