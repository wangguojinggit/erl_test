
%%%-------------------------------------------------------------------
%%% @author wangguojing
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 三月 2018 15:46
%%%-------------------------------------------------------------------
-module(db).
-author("wangguojing").
-include_lib("eunit/include/eunit.hrl").

%% API
%%-export([insert/3, read/2, delete/2, test/0, start/1, db1/1, test1/0,test2/0]).
-compile(export_all).

start() ->
    Pid = spawn(?MODULE, tables, [[]]),
    register(db_tables, Pid).


new_table(Table, {TableType, Perm}) ->
    case Perm of
        priv ->
            Ref = make_ref(),
            db_tables ! {self(), Ref, {add, {Table, {TableType, {priv, self()}}}}},
            receive
                {Ref, Msg} ->
                    Msg
            end;
%%            Pid = spawn(?MODULE, db1, [[], {TableType, {priv, self()}}]),
%%            io:format("start function--- Table: ~p,Pid:~p~n", [Table, Pid]),
%%            register(Table, Pid),
%%            ok;
        pub ->
            Ref = make_ref(),
            db_tables ! {self(), Ref, {add, {Table, {TableType, {pub, self()}}}}},
            receive
                {Ref, Msg} ->
                    Msg
            end
    end.

gen_name(Name, Type, Pid) ->
    list_to_atom(lists:concat([Name, '$', Type, '$', pid_to_list(Pid)])).

get_tab_name({Table, Perm}) when Perm == priv;Perm == pub ->
    Ref = make_ref(),
    db_tables ! {self(), Ref, {get_name, {Table, Perm, self()}}},
    receive
        {Ref, Msg} ->
            Msg
    end;
get_tab_name(_) ->
    undefined.

del_table({Table, Perm}) when Perm == priv;Perm == pub ->
    Ref = make_ref(),
    db_tables ! {self(), Ref, {del, {Table, Perm, self()}}},
    receive
        {Ref, Msg} ->
            Msg
    end;
del_table(_) ->
    table_no_exist.

delete_table({Table, Perm}) ->
%%    io:format("delete table~n"),
    Tab = get_tab_name({Table, Perm}),
    case whereis(Tab) of
        undefined -> table_no_exist;
        _ ->
            Ref = make_ref(),
            Tab ! {self(), Ref, terminate},
            receive
                {Ref, Msg} ->
                    del_table({Table, Perm}),
                    Msg
            after 1000 ->
                timeout
            end
    end.


tables(TableList) ->
    receive
        {From, Ref, {add, {Table, {TableType, {pub, Owner}}}}} ->
            case find({Table, pub, Owner}, TableList) of
                true ->
                    From ! {Ref, table_exist},
                    tables(TableList);
                false ->
                    From ! {Ref, ok},
                    Pid = spawn(?MODULE, db1, [[], {TableType, pub}]),
                    register(gen_name(Table, pub, Owner), Pid),
                    tables([{Table, pub, Owner} | TableList])
            end;
        {From, Ref, {add, {Table, {TableType, {priv, Owner}}}}} ->
            case find({Table, priv, Owner}, TableList) of
                true ->
                    From ! {Ref, table_exist},
                    tables(TableList);
                false ->
                    From ! {Ref, ok},
                    Pid = spawn(?MODULE, db1, [[], {TableType, {priv, Owner}}]),
                    register(gen_name(Table, priv, Owner), Pid),
                    tables([{Table, priv, Owner} | TableList])
            end;
        {From, Ref, {get_name, {Table, Perm, Owner}}} ->
            From ! {Ref, get_name({Table, Perm, Owner}, TableList)},
            tables(TableList);
        {From, Ref, {del, {Table, Perm, Owner}}} ->
            From ! {Ref, ok},
            tables(del({Table, Perm, Owner}, TableList));
        _ ->
            tables(TableList)
    end.

find(_, []) ->
    false;
find({Table, pub, _}, [{Table, pub, _} | _]) ->
    true;
find({Table, priv, Pid}, [{Table, priv, Pid} | _]) ->
    true;
find(Key, [_ | Rest]) ->
    find(Key, Rest).

get_name(_, []) ->
    undefined;
get_name({Table, pub, _}, [{Table, pub, Owner} | _]) ->
    gen_name(Table, pub, Owner);
get_name({Table, priv, Owner}, [{Table, priv, Owner} | _]) ->
    gen_name(Table, priv, Owner);
get_name(Val, [_ | Rest]) ->
    get_name(Val, Rest).


del(_, []) ->
    [];
del({Key, pub, _}, [{Key, pub, _} | Rest]) ->
    Rest;
del({Key, priv, Owner}, [{Key, priv, Owner} | Rest]) ->
    Rest;
del(Key, [H | Rest]) ->
    [H | del(Key, Rest)].

insert({Table, pub}, Key, Val) ->
    Tab = get_tab_name({Table, pub}),
    case whereis(Tab) of
        undefined -> table_no_exist;
        _ ->
            Ref = make_ref(),
            Tab ! {self(), Ref, {insert, {Key, Val}}},
            receive
                {Ref, Msg} ->
                    Msg
            after 1000 ->
                timeout
            end
    end;
insert({Table, priv}, Key, Val) ->
    Tab = get_tab_name({Table, priv}),
    case whereis(Tab) of
        undefined -> table_no_exist;
        _ ->
            Ref = make_ref(),
            Tab ! {self(), Ref, {insert, {Key, Val}}},
            receive
                {Ref, Msg} ->
                    Msg
            after 1000 ->
                timeout
            end
    end.

read({Table, pub}, Key) ->
    Tab = get_tab_name({Table, pub}),
    case whereis(Tab) of
        undefined -> table_no_exist;
        _ ->
            Ref = make_ref(),
            Tab ! {self(), Ref, {read, Key}},
            receive
                {Ref, Msg} ->
                    Msg
            after 1000 ->
                timeout
            end
    end;
read({Table, priv}, Key) ->
    Tab = get_tab_name({Table, priv}),
    case whereis(Tab) of
        undefined -> table_no_exist;
        _ ->
            Ref = make_ref(),
            Tab ! {self(), Ref, {read, Key}},
            receive
                {Ref, Msg} ->
                    Msg
            after 1000 ->
                timeout
            end
    end.

delete({Table, pub}, Key) ->
    Tab = get_tab_name({Table, pub}),
    case whereis(Tab) of
        undefined -> table_no_exist;
        _ ->
            Ref = make_ref(),
            Tab ! {self(), Ref, {delete, Key}},
            receive
                {Ref, Msg} ->
                    Msg
            after 1000 ->
                timeout
            end
    end;
delete({Table, priv}, Key) ->
    Tab = get_tab_name({Table, priv}),
    case whereis(Tab) of
        undefined -> table_no_exist;
        _ ->
            Ref = make_ref(),
            Tab ! {self(), Ref, {delete, Key}},
            receive
                {Ref, Msg} ->
                    Msg
            after 1000 ->
                timeout
            end
    end.




db1(Res, {TableType, {priv, Owner}}) ->
    receive
        {Owner, Ref, {insert, {Key, Val}}} ->
            io:format("db1 function--- Key: ~p,Val: ~p, Res:~p~n", [Key, Val, Res]),
            case insert_db(TableType, Key, Val, Res) of
                false ->
                    Owner ! {Ref, false},
                    db1(Res, {TableType, {priv, Owner}});
                Res1 ->
                    Owner ! {Ref, ok},
                    db1(Res1, {TableType, {priv, Owner}})
            end;
        {Owner, Ref, {read, Key}} ->
            Owner ! {Ref, read_db(TableType, Key, Res)},
            db1(Res, {TableType, {priv, Owner}});
        {Owner, Ref, {delete, Key}} ->
            Owner ! {Ref, ok},
            db1(delete_db(TableType, Key, Res), {TableType, {priv, Owner}});
        {Owner, Ref, terminate} ->
            Owner ! {Ref, ok},
            ok;
        {From, Ref, _} ->
            From ! {Ref, {false, "No permission!"}},
            db1(Res, {TableType, {priv, Owner}})
    end;
db1(Res, {TableType, pub}) ->
    receive
        {From, Ref, {insert, {Key, Val}}} ->
%%            io:format("db1 function--- Key: ~p,Val: ~p, Res:~p~n", [Key, Val, Res]),
            case insert_db(TableType, Key, Val, Res) of
                false ->
                    From ! {Ref, false},
                    db1(Res, {TableType, pub});
                Res1 ->
                    From ! {Ref, ok},
                    db1(Res1, {TableType, pub})
            end;
        {From, Ref, {read, Key}} ->
            From ! {Ref, read_db(TableType, Key, Res)},
            db1(Res, {TableType, pub});
        {From, Ref, {delete, Key}} ->
            From ! {Ref, ok},
            db1(delete_db(TableType, Key, Res), {TableType, pub});
        {Owner, Ref, terminate} ->
            Owner ! {Ref, ok},
            ok;
        _ ->
            db1(Res, {TableType, pub})
    end.







insert_db(_, Key, Val, []) ->
    [{Key, Val}];
insert_db(set, Key, Val, DB) ->
    case read_db(set, Key, DB) of
        [] -> [{Key, Val} | DB];
        [{Key, _}] -> update_db(Key, Val, DB)
    end;
insert_db(bag, Key, Val, DB) ->
    case find(Key, Val, DB) of
        false -> [{Key, Val} | DB];
        ok -> DB
    end.

find(_, _, []) ->
    false;
find(Key, Val, [{Key, Val} | _]) ->
    ok;
find(Key, Val, [_ | Rest]) ->
    find(Key, Val, Rest).


read_db(_, _, []) -> [];
read_db(set, Key, [{Key, Val} | _]) ->
    [{Key, Val}];
read_db(bag, Key, [{Key, Val} | Rest]) ->
    [{Key, Val} | read_db(bag, Key, Rest)];
read_db(TableType, Key, [_H | Rest]) ->
    read_db(TableType, Key, Rest).

delete_db(set, Key, [{Key, _} | Rest]) -> Rest;
delete_db(bag, Key, [{Key, _} | Rest]) ->
    delete_db(bag, Key, Rest);
delete_db(TableType, Key, [H | Rest]) ->
    [H | delete_db(TableType, Key, Rest)];
delete_db(_, _, []) -> [].

update_db(_, _, []) -> [];
update_db(Key, Val, [{Key, _} | Rest]) ->
    [{Key, Val} | Rest];
update_db(Key, Val, [H | Rest]) ->
    [H | update_db(Key, Val, Rest)].


test() ->
    start(),
    Pid_del_table = spawn(?MODULE, del_table_test, []),
    Pid_del_table1 = spawn(?MODULE, del_table_test, []),
    Pid_bag_table = spawn(?MODULE, bag_table_test, []),
    timer:sleep(1000),
    ?assertEqual(ok, new_table(t1, {set, priv})),
    ?assertEqual(table_exist, new_table(t2, {set, pub})),
    ?assertEqual(table_exist, new_table(t2, {bag, pub})),
    Pid_bag_table1 = spawn(?MODULE, process_test, []),

    ?assertEqual(ok,new_table(t1,{set,pub})),
    ?assertEqual(ok, insert({t1, pub}, 1, 4)),
    ?assertEqual([{1, 4}], read({t1, pub}, 1)),
    Pid_pub=spawn(fun()-> ?assertEqual([{1, 4}], read({t1, pub}, 1)) end),
    io:format("~p,~p,~p,~p,~p~n", [Pid_del_table, Pid_del_table1, Pid_bag_table, Pid_bag_table1,Pid_pub]).

del_table_test() ->
    ?assertEqual(ok, new_table(t1, {set, priv})),
    ?assertEqual(ok, insert({t1, priv}, 1, 2)),
    ?assertEqual([{1, 2}], read({t1, priv}, 1)),
    ?assertEqual([], read({t1, priv}, 2)),
    ?assertEqual(ok, insert({t1, priv}, 2, 3)),
    ?assertEqual([{2, 3}], read({t1, priv}, 2)),
    ?assertEqual(ok, insert({t1, priv}, a, 4)),
    ?assertEqual(ok, delete({t1, priv}, 2)),
    ?assertEqual([], read({t1, priv}, 2)),
    ?assertEqual(ok, insert({t1, priv}, 2, t)),
    ?assertEqual(ok, insert({t1, priv}, a, s)),
    ?assertEqual([{a, s}], read({t1, priv}, a)),


    ?assertEqual(ok, delete_table({t1, priv})),

    ?assertEqual(table_no_exist, read({t1, priv}, a)),
    ?assertEqual(table_no_exist, delete({t1, priv}, a)),
    ?assertEqual(table_no_exist, insert({t1, priv}, a, 11)),


    ?assertEqual(ok, new_table(t1, {set, priv})),

    ?assertEqual(ok, insert({t1, priv}, 1, 2)),
    ?assertEqual([{1, 2}], read({t1, priv}, 1)).

bag_table_test() ->
    new_table(t2, {bag, pub}),
    ?assertEqual([], read({t2, pub}, 2)),
    ?assertEqual(ok, insert({t2, pub}, 1, 2)),
    ?assertEqual([{1, 2}], read({t2, pub}, 1)),
    ?assertEqual([], read({t2, pub}, 2)),
    ?assertEqual(ok, insert({t2, pub}, 2, 3)),
    ?assertEqual([{2, 3}], read({t2, pub}, 2)),
    ?assertEqual(ok, insert({t2, pub}, 1, 4)),
    ?assertEqual([{1, 4}, {1, 2}], read({t2, pub}, 1)),
    ?assertEqual(ok, delete({t2, pub}, 2)),
    ?assertEqual([], read({t2, pub}, 2)),
    ?assertEqual(ok, insert({t2, pub}, a, 5)),
    ?assertEqual(ok, insert({t2, pub}, a, 5)),
    ?assertEqual([{a, 5}], read({t2, pub}, a)),
    ?assertEqual(ok, insert({t2, pub}, a, 7)),
    ?assertEqual(ok, insert({t2, pub}, a, 6)),
    ?assertEqual([{a, 6}, {a, 7}, {a, 5}], read({t2, pub}, a)),
    ?assertEqual(ok, delete({t2, pub}, a)),
    ?assertEqual([], read({t2, pub}, a)).


process_test() ->
    ?assertEqual(table_no_exist, read({t1, priv}, a)),
    ?assertEqual(table_no_exist, insert({t1, priv}, a, 2)),
    ?assertEqual(table_no_exist, delete({t1, priv}, a)),
    ?assertEqual([], read({t2, pub}, a)),
    ?assertEqual(ok, insert({t2, pub}, a, 5)),
    ?assertEqual([{a, 5}], read({t2, pub}, a)).

