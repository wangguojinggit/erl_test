
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
-export([insert/2,read/2,delete/2,test/0,start/1,db1/1,test1/1,test2/1]).

start(Res)->
    spawn(?MODULE,db1,[Res]).

insert(Pid,{Key,Val})->
    Pid!{self(),{insert,{Key,Val}}},
    receive
        {Pid,Msg}->
            Msg
    end.

read(Pid,Key)->
    Pid!{self(),{read,Key}},
    receive
        {Pid,Msg}->
            Msg
    end.

delete(Pid,Key)->
    Pid!{self(),{delete,Key}},
    receive
        {Pid,Msg}->
            Msg
    end.


db1(Res)->
    receive
        {From,{insert,{Key,Val}}}->
            case insert_db(Key,Val,Res) of
                false ->
                    From!{self(),false},
                    db1(Res);
                Res1->
                    From!{self(),ok},
                    db1(Res1)
            end;
        {From,{read,Key}}->
            From!{self(),read_db(Key,Res)},
            db1(Res);
        {From,{delete,Key}}->
            From!{self(),ok},
            db1(delete_db(Key,Res));
        terminate->
            ok
    end.





insert_db([],_,_)->
    false;
insert_db('',_,_)->
    false;
insert_db(<<"">>,_,_)->
    false;
insert_db(Key,Val,[])->
    [{Key,Val}];
insert_db(Key,Element,Db)->
    case read_db(Key,Db) of
        [] -> [{Key,Element}|Db];
        {Key,_}->update_db(Key,Element,Db)
    end.


read_db(_,[])->[];
read_db(Key,[{Key,Val}|_])->
    {Key,Val};
read_db(Key,[_H|Rest])->
    read_db(Key,Rest).

delete_db(Key, [{Key,_}|Rest]) -> Rest;
delete_db(Key, [H|Rest]) ->
    [H|delete_db(Key, Rest)];
delete_db(_, []) -> [].

update_db(_,_,[])->[];
update_db(Key,Val,[{Key,_}|Rest])->
    [{Key,Val}|Rest];
update_db(Key,Val,[H|Rest])->
    [H|update_db(Key,Val,Rest)].

test()->
    Pid=start([]),
    io:format("db Pid:~p~n",[Pid]),
    Pid1=spawn(?MODULE,test2,[Pid]),
    Pid2=spawn(?MODULE,test1,[Pid]),
    [{db,Pid},{test2,Pid1},{test1,Pid2}].



test2(Pid)->
    ?assertEqual(false,insert(Pid,{'',2})),
    ?assertEqual(false,insert(Pid,{[],2})),
    ?assertEqual(false,insert(Pid,{<<"">>,2})),
    ?assertEqual(ok,insert(Pid,{1,2})),
    ?assertEqual({1,2},read(Pid,1)),
    ?assertEqual([],read(Pid,2)),
    ?assertEqual(ok,insert(Pid,{2,3})),
    ?assertEqual({2,3},read(Pid,2)),
    ?assertEqual(ok,insert(Pid,{a,4})),
    ?assertEqual(ok,delete(Pid,2)),
    ?assertEqual([],read(Pid,2)),
    ?assertEqual(ok,insert(Pid,{2,t})),
    ?assertEqual(ok,insert(Pid,{a,s})),
    ?assertEqual({a,s},read(Pid,a)).

test1(Pid)->
    timer:sleep(1000),
    ?assertEqual(ok,insert(Pid,{7,12})),
    ?assertEqual({7,12},read(Pid,7)),
    ?assertEqual({a,s},read(Pid,a)).