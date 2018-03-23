
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

start(Table, {TableType, Perm}) ->
    OP = case Perm of
             private -> {Perm, self()};
             public -> public
         end,
    Pid = spawn(?MODULE, db1, [[], {TableType,OP }]),
    io:format("start function--- Table: ~p,Pid:~p~n", [Table, Pid]),
    register(Table, Pid).


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


db1(Res, {TableType, {private, Owner}}) ->
    receive
        {Owner, Ref, {insert, {Key, Val}}} ->
            io:format("db1 function--- Key: ~p,Val: ~p, Res:~p~n", [Key, Val, Res]),
            case insert_db(TableType, Key, Val, Res) of
                false ->
                    Owner ! {Ref, false},
                    db1(Res, {TableType, {private, Owner}});
                Res1 ->
                    Owner ! {Ref, ok},
                    db1(Res1, {TableType, {private, Owner}})
            end;
        {Owner, Ref, {read, Key}} ->
            Owner ! {Ref, read_db(TableType, Key, Res)},
            db1(Res, {TableType, {private, Owner}});
        {Owner, Ref, {delete, Key}} ->
            Owner ! {Ref, ok},
            db1(delete_db(TableType, Key, Res), {TableType, {private, Owner}});
        {From, Ref, _} ->
            From ! {Ref, {false, "No permission!"}},
            db1(Res, {TableType, {private, Owner}})
    end;
db1(Res, {TableType, public}) ->
    receive
        {From, Ref, {insert, {Key, Val}}} ->
            io:format("db1 function--- Key: ~p,Val: ~p, Res:~p~n", [Key, Val, Res]),
            case insert_db(TableType, Key, Val, Res) of
                false ->
                    From ! {Ref, false},
                    db1(Res, {TableType, public});
                Res1 ->
                    From ! {Ref, ok},
                    db1(Res1, {TableType, public})
            end;
        {From, Ref, {read, Key}} ->
            From ! {Ref, read_db(TableType, Key, Res)},
            db1(Res, {TableType, public});
        {From, Ref, {delete, Key}} ->
            From ! {Ref, ok},
            db1(delete_db(TableType, Key, Res), {TableType, public});
        _ ->
            db1(Res, {TableType, public})
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
        true -> DB
    end.

find(_, _, []) ->
    false;
find(Key, Val, [{Key, Val} | _]) ->
    true;
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
    ?assertEqual(true, is_atom(t1)),
    start(t1, {set, private}),
    ?assertEqual(ok, insert(t1, 1, 2)),
    ?assertEqual([{1, 2}], read(t1, 1)),
    ?assertEqual([], read(t1, 2)),
    ?assertEqual(ok, insert(t1, 2, 3)),
    ?assertEqual([{2, 3}], read(t1, 2)),
    ?assertEqual(ok, insert(t1, a, 4)),
    ?assertEqual(ok, delete(t1, 2)),
    ?assertEqual([], read(t1, 2)),
    ?assertEqual(ok, insert(t1, 2, t)),
    ?assertEqual(ok, insert(t1, a, s)),
    ?assertEqual([{a, s}], read(t1, a)),

    start(t2, {bag, public}),
    ?assertEqual([], read(t2, 2)),
    ?assertEqual(ok, insert(t2, 1, 2)),
    ?assertEqual([{1, 2}], read(t2, 1)),
    ?assertEqual([], read(t2, 2)),
    ?assertEqual(ok, insert(t2, 2, 3)),
    ?assertEqual([{2, 3}], read(t2, 2)),
    ?assertEqual(ok, insert(t2, 1, 4)),
    ?assertEqual([{1, 4}, {1, 2}], read(t2, 1)),
    ?assertEqual(ok, delete(t2, 2)),
    ?assertEqual([], read(t2, 2)),
    ?assertEqual(ok, insert(t2, a, 5)),
    ?assertEqual(ok, insert(t2, a, 5)),
    ?assertEqual([{a, 5}], read(t2, a)),
    ?assertEqual(ok, insert(t2, a, 7)),
    ?assertEqual(ok, insert(t2, a, 6)),
    ?assertEqual([{a, 6}, {a, 7}, {a, 5}], read(t2, a)),
    ?assertEqual(ok, delete(t2, a)),
    ?assertEqual([], read(t2, a)).


test2() ->
    ?assertEqual({false, "No permission!"}, read(t1, a)),
    ?assertEqual({false, "No permission!"}, insert(t1, a, 2)),
    ?assertEqual({false, "No permission!"}, delete(t1, a)),
    ?assertEqual([], read(t2, a)),
    ?assertEqual(ok, insert(t2, a, 5)),
    ?assertEqual([{a, 5}], read(t2, a)).

test1() ->
    spawn(?MODULE, test2, []).