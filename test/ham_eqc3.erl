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

-module(ham_eqc3).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include("include/ham.hrl").

-compile(export_all).

-record(dbstate, {handle, % database handle
                flags = [], % database flags for ham_env_create_db
                parameters = [], % database parameters for ham_env_create_db,
                data = [] % ordset with the data
                }).

-record(state, {env_flags = [], % environment flags for ham_env_create
                env_parameters = [], % environment parameters for ham_env_create
                open_dbs = [], % list of open databases [{name, #dbstate}]
                closed_dbs = [] % list of closed databases [{name, #dbstate}]
                }).

run() ->
  eqc:module(?MODULE).


dbname() ->
  choose(1, 3).

dbhandle(State) ->
  elements(State#state.open_dbs).

initial_state() ->
  #state{}.

% ham_env_create_db ---------------------------------

% ignore record_number - it has the same characteristics as HAM_TYPE_UINT64,
% but adds lots of complexity to the test
create_db_flags() ->
  [elements([undefined])].

create_db_parameters() ->
  [
    {record_size, ?HAM_RECORD_SIZE_UNLIMITED},
    {key_type, ?HAM_TYPE_BINARY},
    {key_size, ?HAM_KEY_SIZE_UNLIMITED}
  ].

create_db_pre(State, [_EnvHandle, DbName, _DbFlags, _DbParameters]) ->
  lists:keymember(DbName, 1, State#state.open_dbs) == false
    andalso lists:keymember(DbName, 1, State#state.closed_dbs) == false.

create_db_command(_State) ->
  {call, ?MODULE, create_db, [{var, env}, dbname(), create_db_flags(),
                              create_db_parameters()]}.

create_db(EnvHandle, DbName, DbFlags, DbParams) ->
  case ham:env_create_db(EnvHandle, DbName, DbFlags, lists:flatten(DbParams)) of
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

% ham_env_open_db -----------------------------------
open_db_pre(State, [_EnvHandle, DbName]) ->
  lists:keymember(DbName, 1, State#state.open_dbs) == false
    andalso lists:keymember(DbName, 1, State#state.closed_dbs) == true.

open_db_command(_State) ->
  {call, ?MODULE, open_db, [{var, env}, dbname()]}.

open_db(EnvHandle, DbName) ->
  case ham:env_open_db(EnvHandle, DbName) of
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

% ham_db_insert ------------------------------------
key(_DbState) ->
  binary().

record(_DbState) ->
  binary().

flags(_DbState) ->
  undefined.

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
  ham:db_insert(DbHandle, undefined, Key, Record, Flags).

db_insert_post(State, [{{DbName, _DbHandle}, _Flags, Key, _Record}], Result) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case orddict:find(Key, DbState#dbstate.data) of
    {ok, _} ->
      eq(Result, {error, duplicate_key});
    error ->
      eq(Result, ok)
  end.

db_insert_next(State, Result, [{{DbName, _DbHandle}, _Flags, Key, Record}]) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case Result of
    ok ->
      DbState2 = DbState#dbstate{data
                    = orddict:store(Key, byte_size(Record),
                                DbState#dbstate.data)},
      Databases = lists:keydelete(DbName, 1, State#state.open_dbs),
      State#state{open_dbs = Databases ++ [{DbName, DbState2}]};
    _ ->
      State
  end.

% ham_db_erase -------------------------------------
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
  ham:db_erase(DbHandle, Key).

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

% ham_db_find --------------------------------------
find_params(State) ->
  ?LET(Db, elements(State#state.open_dbs),
    begin
      {DbName, DbState} = Db,
      {{DbName, DbState#dbstate.handle}, key(DbState)}
    end).

db_find_pre(State) ->
  length(State#state.open_dbs) > 0.

db_find_command(State) ->
  {call, ?MODULE, db_find, [erase_params(State)]}.

db_find({{_DbName, DbHandle}, Key}) ->
  ham:db_find(DbHandle, Key).

db_find_post(State, [{{DbName, _DbHandle}, Key}], Result) ->
  {DbName, DbState} = lists:keyfind(DbName, 1, State#state.open_dbs),
  case orddict:find(Key, DbState#dbstate.data) of
    {ok, RecordSize} ->
      {ok, Record} = Result,
      eq(byte_size(Record), RecordSize);
    error ->
      eq(Result, {error, key_not_found})
  end.

db_find_next(State, _, _) ->
  State.

% ham_env_close_db ---------------------------------
db_close_pre(State) ->
  length(State#state.open_dbs) > 0.

db_close_command(State) ->
  {call, ?MODULE, db_close, [dbhandle(State)]}.

db_close({_DbName, DbState}) ->
  ham:db_close(DbState#dbstate.handle).

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
  [undefined].

env_parameters() ->
  [{cache_size, 100000}, {page_size, 1024}].

weight(_State, db_insert) ->
  100;
weight(_State, db_erase) ->
  100;
weight(_State, _) ->
  10.

prop_ham3() ->
  ?FORALL({EnvFlags, EnvParams}, {env_flags(), env_parameters()},
    ?FORALL(Cmds, more_commands(500,
              commands(?MODULE, #state{env_flags = EnvFlags,
                              env_parameters = EnvParams})),
      begin
        % io:format("flags ~p, params ~p ~n", [EnvFlags, EnvParams]),
        {ok, EnvHandle} = ham:env_create("ham_eqc.db", EnvFlags, 0,
                                       EnvParams),
        {History, State, Result} = run_commands(?MODULE, Cmds,
                                            [{env, EnvHandle}]),
        eqc_statem:show_states(
          pretty_commands(?MODULE, Cmds, {History, State, Result},
            aggregate(command_names(Cmds),
              %%collect(length(Cmds),
                begin
                  ham:env_close(EnvHandle),
                  Result == ok
                end)))%%)
      end)).

