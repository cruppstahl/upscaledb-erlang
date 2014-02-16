%%
%% This program is free software; you can redistribute it and/or modify it
%% under the terms of the GNU General Public License as published by the
%% Free Software Foundation; either version 2 of the License, or
%% (at your option) any later version.
%%
%% See files COPYING.* for License information.
%%
-module(ham_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("include/ham.hrl").

run_test_() ->
  {timeout, 60, [
    ?_test(db1()),
    ?_test(env1()),
    ?_test(txn1()),
    ?_test(cursor1())
   ]}.

%%
%% This test is similar to sample/db1.c - it demonstrates the basic hamsterdb
%% flow (inserting, deleting and looking up key/value pairs).
%%
db1() ->
  %% First step: create a new Environment
  {ok, Env1} = ham:env_create("test.db"),
  %% Then create a Database in this Environment with the name '1', and for
  %% uint32 keys
  {ok, Db1} = ham:env_create_db(Env1, 1, [], [{key_type, ?HAM_TYPE_UINT32}]),
  %% Now insert 10 key/value pairs
  lists:foreach(fun(I) -> ok = ham:db_insert(Db1, 
                                         list_to_binary(integer_to_list(I)),
                                         <<"Record">>)
                end, lists:seq(1, 10)),
  %% Close the Database and the Environment
  ok = ham:db_close(Db1),
  ok = ham:env_close(Env1),
  %% Now reopen both
  {ok, Env2} = ham:env_open("test.db"),
  {ok, Db2} = ham:env_open_db(Env2, 1),
  %% Now lookup those keys
  lists:foreach(fun(I) -> ?assertEqual({ok, <<"Record">>}, ham:db_find(Db2,
                                         list_to_binary(integer_to_list(I))))
                end, lists:seq(1, 10)),
  %% Delete them
  lists:foreach(fun(I) -> ok = ham:db_erase(Db2, 
                                         list_to_binary(integer_to_list(I)))
                end, lists:seq(1, 10)),
  %% Look them up again - must fail
  lists:foreach(fun(I) -> ?assertEqual({error, key_not_found}, ham:db_find(Db2,
                                         list_to_binary(integer_to_list(I))))
                end, lists:seq(1, 10)),
  %% and close Database and Environment again
  ok = ham:db_close(Db2),
  ok = ham:env_close(Env2),
  true.

%%
%% This test demonstrates how to use multiple Databases in an Environment.
%%
env1() ->
  %% First step: create a new Environment
  {ok, Env1} = ham:env_create("test.db"),
  %% Then create a few Databases in this Environment, each with an unique name
  {ok, Db1} = ham:env_create_db(Env1, 1),
  {ok, Db2} = ham:env_create_db(Env1, 2),
  {ok, Db3} = ham:env_create_db(Env1, 3),
  %% We could now insert or delete or lookup key/value pairs in any of these
  %% Databases...
  %%
  %% Close the Databases
  ok = ham:db_close(Db1),
  ok = ham:db_close(Db2),
  ok = ham:db_close(Db3),
  %% Now we could delete Database #1
  ok = ham:env_erase_db(Env1, 1),
  %% ... or rename the Database #2 as #5
  ok = ham:env_rename_db(Env1, 2, 5),
  %% Close the Environment
  ok = ham:env_close(Env1),
  true.

%%
%% This test demonstrates how to use Transactions. It begins, aborts and
%% commits various Transactions.
%%
txn1() ->
  %% First step: create a new Environment, with Transactions enabled
  {ok, Env1} = ham:env_create("test.db", [enable_transactions]),
  %% Then create a Databases in this Environment
  {ok, Db1} = ham:env_create_db(Env1, 1),
  %% Start a new Transaction
  {ok, Txn1} = ham:txn_begin(Env1),
  %% Insert a few key/value pair in that Transaction
  ok = ham:db_insert(Db1, Txn1, <<"foo1">>, <<"value1">>),
  ok = ham:db_insert(Db1, Txn1, <<"foo2">>, <<"value2">>),
  ok = ham:db_insert(Db1, Txn1, <<"foo3">>, <<"value3">>),
  %% Commit the Transaction
  ok = ham:txn_commit(Txn1),

  %% Now start another Transaction, again insert a few values but then
  %% *abort* this Transaction
  {ok, Txn2} = ham:txn_begin(Env1),
  ok = ham:db_insert(Db1, Txn2, <<"bar1">>, <<"value1">>),
  ok = ham:db_insert(Db1, Txn2, <<"bar2">>, <<"value2">>),
  ok = ham:db_insert(Db1, Txn2, <<"bar3">>, <<"value3">>),
  ok = ham:txn_abort(Txn2),

  %% Now verify that Txn1 was committed, Txn2 was aborted
  ?assertEqual({ok, <<"value1">>}, ham:db_find(Db1, <<"foo1">>)),
  ?assertEqual({ok, <<"value2">>}, ham:db_find(Db1, <<"foo2">>)),
  ?assertEqual({ok, <<"value3">>}, ham:db_find(Db1, <<"foo3">>)),
  ?assertEqual({error, key_not_found}, ham:db_find(Db1, <<"bar1">>)),
  ?assertEqual({error, key_not_found}, ham:db_find(Db1, <<"bar2">>)),
  ?assertEqual({error, key_not_found}, ham:db_find(Db1, <<"bar3">>)),

  %% Close the Database and the Environment
  ok = ham:db_close(Db1),
  ok = ham:env_close(Env1),
  true.

%%
%% This test demonstrates how to use Cursors. It inserts key/value pairs and
%% then traverses from both directions.
%%
cursor1() ->
  %% First step: create a new Environment
  {ok, Env1} = ham:env_create("test.db", [enable_transactions]),
  %% Then create a Databases in this Environment
  {ok, Db1} = ham:env_create_db(Env1, 1),
  %% And a Cursor for this Database
  {ok, Cursor1} = ham:cursor_create(Db1),
  %% Insert a few key/value pairs with this Cursor (we could use
  %% ham:db_insert() as well)
  ok = ham:cursor_insert(Cursor1, <<"foo1">>, <<"value1">>),
  ok = ham:cursor_insert(Cursor1, <<"foo2">>, <<"value2">>),
  ok = ham:cursor_insert(Cursor1, <<"foo3">>, <<"value3">>),
  ok = ham:cursor_insert(Cursor1, <<"foo4">>, <<"value4">>),
  ok = ham:cursor_insert(Cursor1, <<"foo5">>, <<"value5">>),
  %% Now traverse from beginning to the end
  {ok, <<"foo1">>, <<"value1">>} = ham:cursor_move(Cursor1, [first]),
  {ok, <<"foo2">>, <<"value2">>} = ham:cursor_move(Cursor1, [next]),
  {ok, <<"foo3">>, <<"value3">>} = ham:cursor_move(Cursor1, [next]),
  {ok, <<"foo4">>, <<"value4">>} = ham:cursor_move(Cursor1, [next]),
  {ok, <<"foo5">>, <<"value5">>} = ham:cursor_move(Cursor1, [next]),
  {error, key_not_found} = ham:cursor_move(Cursor1, [next]),
  %% and backwards
  {ok, <<"foo5">>, <<"value5">>} = ham:cursor_move(Cursor1, [last]),
  {ok, <<"foo4">>, <<"value4">>} = ham:cursor_move(Cursor1, [previous]),
  {ok, <<"foo3">>, <<"value3">>} = ham:cursor_move(Cursor1, [previous]),
  {ok, <<"foo2">>, <<"value2">>} = ham:cursor_move(Cursor1, [previous]),
  {ok, <<"foo1">>, <<"value1">>} = ham:cursor_move(Cursor1, [previous]),
  {error, key_not_found} = ham:cursor_move(Cursor1, [previous]),

  %% Get the record size
  {ok, <<"foo1">>, <<"value1">>} = ham:cursor_move(Cursor1, [first]),
  {ok, 6} = ham:cursor_get_record_size(Cursor1),
  %% "foo1" has one record assigned
  {ok, 1} = ham:cursor_get_duplicate_count(Cursor1),
  %% Now delete "foo1"
  ok = ham:cursor_erase(Cursor1),
  {ok, <<"foo2">>, <<"value2">>} = ham:cursor_move(Cursor1, [first]),

  %% Clean up
  ok = ham:cursor_close(Cursor1),
  ok = ham:db_close(Db1),
  ok = ham:env_close(Env1),
  true.

-endif.
