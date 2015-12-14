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
    ?_test(uqi1()),
    ?_test(uqi2())
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
%% This test demonstrates the UQI (upscaledb query interface).
%%
uqi1() ->
  %% First step: create a new Environment
  {ok, Env1} = ups:env_create("test.db", []),
  %% Then create a Databases in this Environment
  {ok, Db1} = ups:env_create_db(Env1, 1),
  ok = ups:db_insert(Db1, <<"foo1">>, <<"value1">>),
  ok = ups:db_insert(Db1, <<"foo2">>, <<"value2">>),
  ok = ups:db_insert(Db1, <<"foo3">>, <<"value3">>),
  ok = ups:db_insert(Db1, <<"foo4">>, <<"value4">>),
  ok = ups:db_insert(Db1, <<"foo5">>, <<"value5">>),
  %% Now count them with UQI
  {ok, Result} = ups:select_range(Env1, "COUNT($key) from database 1"),
  %% We will have one result (a 64bit integer) with the actual counter
  {ok, 1} = ups:result_get_row_count(Result),
  {ok, ?UPS_TYPE_UINT64} = ups:result_get_record_type(Result),
  {ok, <<5:64/little>>} = ups:result_get_record(Result, 0),
  %% Clean up
  ok = ups:result_close(Result),
  ok = ups:db_close(Db1),
  ok = ups:env_close(Env1),
  true.

%%
%% This test demonstrates the pagination features of UQI
%% (upscaledb query interface).
%%
uqi2() ->
  %% First step: create a new Environment
  {ok, Env1} = ups:env_create("test.db", []),
  %% Then create a Databases in this Environment
  {ok, Db1} = ups:env_create_db(Env1, 1, [], [{key_type, ?UPS_TYPE_UINT32}]),
  ok = ups:db_insert(Db1, <<1:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<2:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<3:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<4:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<5:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<6:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<7:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<8:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<9:32/little>>, <<"value1">>),
  ok = ups:db_insert(Db1, <<10:32/little>>, <<"value1">>),
  %% Now aggregate them with UQI
  {ok, Result} = ups:select_range(Env1, "SUM($key) from database 1"),
  %% We will have one result (a 64bit integer) with the aggregated sum
  {ok, 1} = ups:result_get_row_count(Result),
  {ok, ?UPS_TYPE_UINT64} = ups:result_get_record_type(Result),
  {ok, <<55:64/little>>} = ups:result_get_record(Result, 0),

  %% Now create a cursor and only aggregate the second half
  {ok, Cursor} = ups:cursor_create(Db1),
  {ok, _} = ups:cursor_find(Cursor, <<5:32/little>>),

  {ok, Result2} = ups:select_range(Env1, "SUM($key) from database 1", Cursor),
  {ok, <<45:64/little>>} = ups:result_get_record(Result2, 0),

  %% The cursor was now moved to the end of the database
  {error, key_not_found} = ups:cursor_move(Cursor, [next]),

  %% Clean up
  ok = ups:cursor_close(Cursor),
  ok = ups:result_close(Result),
  ok = ups:result_close(Result2),
  ok = ups:db_close(Db1),
  ok = ups:env_close(Env1),
  true.
-endif.
