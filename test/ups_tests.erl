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
%%

-module(ups_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("include/ups.hrl").

run_test_() ->
  {timeout, 60, [
    ?_test(db1()),
    ?_test(env1()),
    ?_test(txn1()),
    ?_test(cursor1()),
    ?_test(uqi1())
   ]}.

%%
%% This test is similar to sample/db1.c - it demonstrates the basic upscaledb
%% flow (inserting, deleting and looking up key/value pairs).
%%
db1() ->
  %% First step: create a new Environment
  {ok, Env1} = ups:env_create("test.db"),
  %% Then create a Database in this Environment with the name '1'
  {ok, Db1} = ups:env_create_db(Env1, 1),
  %% Now insert 10 key/value pairs
  lists:foreach(fun(I) -> ok = ups:db_insert(Db1, 
                                         list_to_binary(integer_to_list(I)),
                                         <<"Record">>)
                end, lists:seq(1, 10)),
  %% Close the Database and the Environment
  ok = ups:db_close(Db1),
  ok = ups:env_close(Env1),
  %% Now reopen both
  {ok, Env2} = ups:env_open("test.db"),
  {ok, Db2} = ups:env_open_db(Env2, 1),
  %% Now lookup those keys
  lists:foreach(fun(I) -> ?assertEqual({ok, <<"Record">>}, ups:db_find(Db2,
                                         list_to_binary(integer_to_list(I))))
                end, lists:seq(1, 10)),
  %% Delete them
  lists:foreach(fun(I) -> ok = ups:db_erase(Db2, 
                                         list_to_binary(integer_to_list(I)))
                end, lists:seq(1, 10)),
  %% Look them up again - must fail
  lists:foreach(fun(I) -> ?assertEqual({error, key_not_found}, ups:db_find(Db2,
                                         list_to_binary(integer_to_list(I))))
                end, lists:seq(1, 10)),
  %% and close Database and Environment again
  ok = ups:db_close(Db2),
  ok = ups:env_close(Env2),
  true.

%%
%% This test demonstrates how to use multiple Databases in an Environment.
%%
env1() ->
  %% First step: create a new Environment
  {ok, Env1} = ups:env_create("test.db"),
  %% Then create a few Databases in this Environment, each with an unique name
  {ok, Db1} = ups:env_create_db(Env1, 1),
  {ok, Db2} = ups:env_create_db(Env1, 2),
  {ok, Db3} = ups:env_create_db(Env1, 3),
  %% We could now insert or delete or lookup key/value pairs in any of these
  %% Databases...
  %%
  %% Close the Databases
  ok = ups:db_close(Db1),
  ok = ups:db_close(Db2),
  ok = ups:db_close(Db3),
  %% Now we could delete Database #1
  ok = ups:env_erase_db(Env1, 1),
  %% ... or rename the Database #2 as #5
  ok = ups:env_rename_db(Env1, 2, 5),
  %% Close the Environment
  ok = ups:env_close(Env1),
  true.

%%
%% This test demonstrates how to use Transactions. It begins, aborts and
%% commits various Transactions.
%%
txn1() ->
  %% First step: create a new Environment, with Transactions enabled
  {ok, Env1} = ups:env_create("test.db", [enable_transactions]),
  %% Then create a Databases in this Environment
  {ok, Db1} = ups:env_create_db(Env1, 1),
  %% Start a new Transaction
  {ok, Txn1} = ups:txn_begin(Env1),
  %% Insert a few key/value pair in that Transaction
  ok = ups:db_insert(Db1, Txn1, <<"foo1">>, <<"value1">>),
  ok = ups:db_insert(Db1, Txn1, <<"foo2">>, <<"value2">>),
  ok = ups:db_insert(Db1, Txn1, <<"foo3">>, <<"value3">>),
  %% Commit the Transaction
  ok = ups:txn_commit(Txn1),

  %% Now start another Transaction, again insert a few values but then
  %% *abort* this Transaction
  {ok, Txn2} = ups:txn_begin(Env1),
  ok = ups:db_insert(Db1, Txn2, <<"bar1">>, <<"value1">>),
  ok = ups:db_insert(Db1, Txn2, <<"bar2">>, <<"value2">>),
  ok = ups:db_insert(Db1, Txn2, <<"bar3">>, <<"value3">>),
  ok = ups:txn_abort(Txn2),

  %% Now verify that Txn1 was committed, Txn2 was aborted
  ?assertEqual({ok, <<"value1">>}, ups:db_find(Db1, <<"foo1">>)),
  ?assertEqual({ok, <<"value2">>}, ups:db_find(Db1, <<"foo2">>)),
  ?assertEqual({ok, <<"value3">>}, ups:db_find(Db1, <<"foo3">>)),
  ?assertEqual({error, key_not_found}, ups:db_find(Db1, <<"bar1">>)),
  ?assertEqual({error, key_not_found}, ups:db_find(Db1, <<"bar2">>)),
  ?assertEqual({error, key_not_found}, ups:db_find(Db1, <<"bar3">>)),

  %% Close the Database and the Environment
  ok = ups:db_close(Db1),
  ok = ups:env_close(Env1),
  true.

%%
%% This test demonstrates how to use Cursors. It inserts key/value pairs and
%% then traverses from both directions.
%%
cursor1() ->
  %% First step: create a new Environment
  {ok, Env1} = ups:env_create("test.db", [enable_transactions]),
  %% Then create a Databases in this Environment
  {ok, Db1} = ups:env_create_db(Env1, 1),
  %% And a Cursor for this Database
  {ok, Cursor1} = ups:cursor_create(Db1),
  %% Insert a few key/value pairs with this Cursor (we could use
  %% ups:db_insert() as well)
  ok = ups:cursor_insert(Cursor1, <<"foo1">>, <<"value1">>),
  ok = ups:cursor_insert(Cursor1, <<"foo2">>, <<"value2">>),
  ok = ups:cursor_insert(Cursor1, <<"foo3">>, <<"value3">>),
  ok = ups:cursor_insert(Cursor1, <<"foo4">>, <<"value4">>),
  ok = ups:cursor_insert(Cursor1, <<"foo5">>, <<"value5">>),
  %% Now traverse from beginning to the end
  {ok, <<"foo1">>, <<"value1">>} = ups:cursor_move(Cursor1, [first]),
  {ok, <<"foo2">>, <<"value2">>} = ups:cursor_move(Cursor1, [next]),
  {ok, <<"foo3">>, <<"value3">>} = ups:cursor_move(Cursor1, [next]),
  {ok, <<"foo4">>, <<"value4">>} = ups:cursor_move(Cursor1, [next]),
  {ok, <<"foo5">>, <<"value5">>} = ups:cursor_move(Cursor1, [next]),
  {error, key_not_found} = ups:cursor_move(Cursor1, [next]),
  %% and backwards
  {ok, <<"foo5">>, <<"value5">>} = ups:cursor_move(Cursor1, [last]),
  {ok, <<"foo4">>, <<"value4">>} = ups:cursor_move(Cursor1, [previous]),
  {ok, <<"foo3">>, <<"value3">>} = ups:cursor_move(Cursor1, [previous]),
  {ok, <<"foo2">>, <<"value2">>} = ups:cursor_move(Cursor1, [previous]),
  {ok, <<"foo1">>, <<"value1">>} = ups:cursor_move(Cursor1, [previous]),
  {error, key_not_found} = ups:cursor_move(Cursor1, [previous]),

  %% Get the record size
  {ok, <<"foo1">>, <<"value1">>} = ups:cursor_move(Cursor1, [first]),
  {ok, 6} = ups:cursor_get_record_size(Cursor1),
  %% "foo1" has one record assigned
  {ok, 1} = ups:cursor_get_duplicate_count(Cursor1),
  %% Now delete "foo1"
  ok = ups:cursor_erase(Cursor1),
  {ok, <<"foo2">>, <<"value2">>} = ups:cursor_move(Cursor1, [first]),

  %% Clean up
  ok = ups:cursor_close(Cursor1),
  ok = ups:db_close(Db1),
  ok = ups:env_close(Env1),
  true.

%%
%% This test performs a couple of UQI queries.
%%
uqi1() ->
  %% First step: create a new Environment
  {ok, Env1} = ups:env_create("test.db"),
  %% Then create a Database in this Environment with the name '1', storing
  %% 32bit integers as records
  {ok, Db1} = ups:env_create_db(Env1, 1, [], [{record_type, ?UPS_TYPE_UINT32}]),

  %% Now insert 10000 key/value pairs
  lists:foreach(fun(I) ->
                        V = 50 + I rem 30,
                        ok = ups:db_insert(Db1, <<I:32>>, <<V:32>>)
                end, lists:seq(1, 10000)),

  %% Retrieve the maximum record value
  {ok, Result1} = ups:uqi_select_range(Env1, "MAX($record) FROM DATABASE 1"),
  ?assertEqual({ok, 1}, ups:uqi_result_get_row_count(Result1)),
  ?assertEqual({ok, 0}, ups:uqi_result_get_key_type(Result1)),
  ?assertEqual({ok, 7}, ups:uqi_result_get_record_type(Result1)),
  ?assertEqual({ok,<<0,0,0,79>>}, ups:uqi_result_get_record(Result1, 0)),
  ok = ups:uqi_result_close(Result1),

  %% Retrieve the minimum record value
  {ok, Result2} = ups:uqi_select_range(Env1, "MIN($record) FROM DATABASE 1"),
  ?assertEqual({ok, 1}, ups:uqi_result_get_row_count(Result2)),
  ?assertEqual({ok, 0}, ups:uqi_result_get_key_type(Result2)),
  ?assertEqual({ok, 7}, ups:uqi_result_get_record_type(Result2)),
  ?assertEqual({ok,<<0,0,0,50>>}, ups:uqi_result_get_record(Result2, 0)),
  ok = ups:uqi_result_close(Result2),

  ok = ups:db_close(Db1),
  ok = ups:env_close(Env1),
  true.

-endif.
