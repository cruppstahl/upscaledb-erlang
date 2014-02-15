%%
%% Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
%%
%% This program is free software; you can redistribute it and/or modify it
%% under the terms of the GNU General Public License as published by the
%% Free Software Foundation; either version 2 of the License, or
%% (at your option) any later version.
%%
%% See files COPYING.* for License information.
%%
%%
-module(ham_eqc2).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

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

run_test_() ->
  {timeout, 60, [fun() -> run() end]}.

run() ->
  eqc:module(?MODULE).


dbname() ->
  choose(1, 5).

initial_state() ->
  #state{}.

% ham_env_create_db ---------------------------------

% ignore record_number - it has the same characteristics as HAM_TYPE_UINT64,
% but adds lots of complexity to the test
create_db_flags() ->
  [elements([undefined, enable_duplicate_keys])].

create_db_parameters() ->
  ?LET(KeySize, choose(1, 128),
    ?LET(RecordSize, choose(0, 16),
      [
        oneof([
                {record_size, ?HAM_RECORD_SIZE_UNLIMITED},
                {record_size, RecordSize}
              ]),
        oneof([
                [{key_type, ?HAM_TYPE_BINARY}, {key_size,
                                                ?HAM_KEY_SIZE_UNLIMITED}],
                [{key_type, ?HAM_TYPE_BINARY}, {key_size, KeySize}],
                {key_type, ?HAM_TYPE_UINT8},
                {key_type, ?HAM_TYPE_UINT16},
                {key_type, ?HAM_TYPE_UINT32},
                {key_type, ?HAM_TYPE_UINT64},
                {key_type, ?HAM_TYPE_REAL32},
                {key_type, ?HAM_TYPE_REAL64}
              ])
      ])).

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

% ham_env_erase_db ---------------------------------
erase_db_pre(State, [_EnvHandle, DbName]) ->
  lists:keymember(DbName, 1, State#state.open_dbs) == false
    andalso lists:keymember(DbName, 1, State#state.closed_dbs) == true.

erase_db_command(_State) ->
  {call, ?MODULE, erase_db, [{var, env}, dbname()]}.

erase_db(EnvHandle, DbName) ->
  ham:env_erase_db(EnvHandle, DbName).

erase_db_post(_State, [_EnvHandle, _DbName], Result) ->
  Result == ok.

erase_db_next(State, _Result, [_EnvHandle, DbName]) ->
  State#state{open_dbs = lists:keydelete(DbName, 1, State#state.open_dbs),
        closed_dbs = lists:keydelete(DbName, 1, State#state.closed_dbs)}.

% ham_db_insert ------------------------------------
key(DbState) ->
  case lists:keyfind(key_type, 1, DbState#dbstate.parameters) of 
    {key_type, ?HAM_TYPE_UINT8} ->
      binary(1);
    {key_type, ?HAM_TYPE_UINT16} ->
      binary(2);
    {key_type, ?HAM_TYPE_UINT32} ->
      binary(4);
    {key_type, ?HAM_TYPE_UINT64} ->
      binary(8);
    {key_type, ?HAM_TYPE_REAL32} ->
      binary(4);
    {key_type, ?HAM_TYPE_REAL64} ->
      binary(8);
    _ ->
      case lists:keyfind(key_size, 1, DbState#dbstate.parameters) of 
        {key_size, ?HAM_KEY_SIZE_UNLIMITED} ->
          binary();
        {key_size, N} ->
          binary(N);
        _ ->
          binary()
      end
  end.

record(DbState) ->
  case lists:keyfind(record_size, 1, DbState#dbstate.parameters) of 
    {record_size, ?HAM_RECORD_SIZE_UNLIMITED} ->
        binary();
    {record_size, N} ->
        binary(N)
  end.

db_insert_pre(State) ->
  length(State#state.open_dbs) > 0.

db_insert_command(State) ->
  {call, ?MODULE, db_insert, [elements(State#state.open_dbs), 
                              binary(), binary()]}.

db_insert({_DbName, DbState}, Key, Record) ->
  ham:db_insert(DbState#dbstate.handle, Key, Record).

db_insert_post(_State, [{_DbName, _DbState}, _Key, _Record], _Result) ->
  true.
  %case orddict:find(Key, DbState#dbstate.data) of
    %{ok, _} ->
      %eq(Result, {error, duplicate_key});
    %error ->
      %eq(Result, ok)
  %end.

db_insert_next(State, Result, [{_DbName, _DbState}, _Key, _Record]) ->
  case Result of
    ok ->
      %DbState2 = DbState#dbstate{data
                    %= orddict:store(Key, Record, DbState#dbstate.data)},
      %Databases = lists:keydelete(DbName, 1, State#state.open_dbs),
      %State#state{open_dbs = Databases ++ [{DbName, DbState2}]};
      State;
    _ ->
      State
  end.

% ham_env_close_db ---------------------------------
db_close_pre(State) ->
  length(State#state.open_dbs) > 0.

db_close_command(State) ->
  {call, ?MODULE, db_close, [elements(State#state.open_dbs)]}.

db_close({_DbName, DbState}) ->
  ham:db_close(DbState#dbstate.handle).

db_close_post(_State, [_Database], Result) ->
  Result == ok.

db_close_next(State, _Result, [{DbName, DbState}]) ->
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
    list(elements([enable_fsync, disable_mmap, cache_unlimited,
                        enable_recovery]))]).

env_parameters() ->
  list(elements([{cache_size, 100000}, {page_size, 1024 * 4}])).

is_valid_combination(EnvFlags, EnvParams) ->
  not (lists:member(in_memory, EnvFlags)
    andalso lists:keymember(cache_size, 1, EnvParams)).

weight(_State, db_insert) ->
  100;
weight(_State, _) ->
  10.

prop_ham() ->
  ?FORALL({EnvFlags, EnvParams}, {env_flags(), env_parameters()},
    ?IMPLIES(is_valid_combination(EnvFlags, EnvParams),
      ?FORALL(Cmds, commands(?MODULE, #state{env_flags = EnvFlags,
                                           env_parameters = EnvParams}),
        begin
          % io:format("flags ~p, params ~p ~n", [EnvFlags, EnvParams]),
          {ok, EnvHandle} = ham:env_create("ham_eqc.db", EnvFlags, 0,
                                         EnvParams),
          {History, State, Result} = run_commands(?MODULE, Cmds,
                                              [{env, EnvHandle}]),
          eqc_statem:show_states(
            pretty_commands(?MODULE, Cmds, {History, State, Result},
              aggregate(command_names(Cmds),
                collect(length(Cmds),
                  begin
                    ham:env_close(EnvHandle),
                    Result == ok
                  end))))
        end))).

