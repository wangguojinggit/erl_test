
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
-export([insert/3, read/2, delete/2, test/0, start/1, db1/1, test1/0,test2/0]).

start(Table) ->
    Pid=spawn(?MODULE, db1, [[]]),
    io:format("start function--- Table: ~p,Pid:~p~n",[Table,Pid]),
    register(Table, Pid).


%%restarter(Table) ->
%%%%    process_flag(trap_exit,true),
%%    Pid = spawn_link(?MODULE, db1, [[]]),
%%    ?assertEqual(true,is_atom(Table)),
%%    register(Table, Pid),
%%    receive
%%        {'EXIT', Pid, normal} ->
%%            ok;
%%        {'EXIT', Pid, shutdown} ->
%%            ok;
%%        {'EXIT', Pid, _} ->
%%            false
%%    end.

insert(Table, Key, Val) ->
    Ref = make_ref(),
    Table ! {self(), Ref, {insert, {Key, Val}}},
    receive
        {Ref, Msg} ->
            Msg
    end.

read(Table, Key) ->
    Ref = make_ref(),
    Table ! {self(), Ref, {read, Key}},
    receive
        {Ref, Msg} ->
            Msg
    end.

delete(Table, Key) ->
    Ref = make_ref(),
    Table ! {self(), Ref, {delete, Key}},
    receive
        {Ref, Msg} ->
            Msg
    end.


db1(Res) ->
    receive
        {From, Ref, {insert, {Key, Val}}} ->
            io:format("db1 function--- Key: ~p,Val: ~p, Res:~p~n",[Key,Val,Res]),
            case insert_db(Key, Val, Res) of
                false ->
                    From ! {Ref, false},
                    db1(Res);
                Res1 ->
                    From ! {Ref, ok},
                    db1(Res1)
            end;
        {From, Ref, {read, Key}} ->
            From ! {Ref, read_db(Key, Res)},
            db1(Res);
        {From, Ref, {delete, Key}} ->
            From ! {Ref, ok},
            db1(delete_db(Key, Res));
        terminate ->
            ok
    end.





insert_db([], _, _) ->
    false;
insert_db('', _, _) ->
    false;
insert_db(<<"">>, _, _) ->
    false;
insert_db(Key, Val, []) ->
    [{Key, Val}];
insert_db(Key, Element, Db) ->
    case read_db(Key, Db) of
        [] -> [{Key, Element} | Db];
        {Key, _} -> update_db(Key, Element, Db)
    end.


read_db(_, []) -> [];
read_db(Key, [{Key, Val} | _]) ->
    {Key, Val};
read_db(Key, [_H | Rest]) ->
    read_db(Key, Rest).

delete_db(Key, [{Key, _} | Rest]) -> Rest;
delete_db(Key, [H | Rest]) ->
    [H | delete_db(Key, Rest)];
delete_db(_, []) -> [].

update_db(_, _, []) -> [];
update_db(Key, Val, [{Key, _} | Rest]) ->
    [{Key, Val} | Rest];
update_db(Key, Val, [H | Rest]) ->
    [H | update_db(Key, Val, Rest)].

test() ->
    ?assertEqual(true,is_atom(t1)),
    start(t1),
    ?assertEqual(false, insert(t1, '', 2)),
    ?assertEqual(false, insert(t1, [], 2)),
    ?assertEqual(false, insert(t1, <<"">>, 2)),
    ?assertEqual(ok, insert(t1, 1, 2)),
    ?assertEqual({1, 2}, read(t1, 1)),
    ?assertEqual([], read(t1, 2)),
    ?assertEqual(ok, insert(t1, 2, 3)),
    ?assertEqual({2, 3}, read(t1, 2)),
    ?assertEqual(ok, insert(t1, a, 4)),
    ?assertEqual(ok, delete(t1, 2)),
    ?assertEqual([], read(t1, 2)),
    ?assertEqual(ok, insert(t1, 2, t)),
    ?assertEqual(ok, insert(t1, a, s)),
    ?assertEqual({a, s}, read(t1, a)).


test2()->
    ?assertEqual({a,s},read(t1,a)).

test1() ->
    spawn(?MODULE, test2, []).