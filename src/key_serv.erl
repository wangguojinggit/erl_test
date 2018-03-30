%%%-------------------------------------------------------------------
%%% @author wangguojing
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 三月 2018 8:42
%%%-------------------------------------------------------------------
-module(key_serv).
-author("wangguojing").

%% API
-export([start/1, loop/1, request/0, return/1, test/0]).

start(MaxNumKey) ->
    register(?MODULE, Pid = spawn(?MODULE, loop, [init(MaxNumKey)])),
    Pid.

init(MaxNumKey) ->
    KeyList = [{X, unlock} || X <- lists:seq(1, MaxNumKey)],
    KeyList.

request() ->
    Ref = make_ref(),
    ?MODULE ! {self(), Ref, {request}},
    receive
        {Ref, Msg} ->
            Msg
    after 2000 ->
        timeout
    end.

return(Key) ->
    Ref = make_ref(),
    ?MODULE ! {self(), Ref, {return, Key}},
    receive
        {Ref, Msg} ->
            Msg
    after 2000 ->
        timeout
    end.


loop(Res) ->
    io:format("Res: ~p~n",[Res]),
    receive
        {From, Ref, {request}} ->
            case find_key(Res) of
                {ok, Key} ->
                    From ! {Ref, {ok, Key}},
                    loop(update_key({Key, lock}, Res));
                false ->
                    From ! {Ref, {error, no_key}},
                    loop(Res)
            end;
        {From, Ref, {return, Key}} ->
            From ! {Ref, ok},
            loop(update_key({Key, unlock}, Res));
        _ ->
            loop(Res)
    end.

find_key([]) ->
    false;
find_key([{Key, unlock} | _]) ->
    {ok, Key};
find_key([H | Rest]) ->
    find_key(Rest).

update_key(_, []) ->
    [];
update_key({Key, State}, [{Key, _} | Rest]) ->
    [{Key, State} | Rest];
update_key({Key, State}, [H | Rest]) ->
    [H | update_key({Key, State}, Rest)].

test() ->
    [{1, unlock}, {2, unlock}, {3, unlock}, {4, unlock}] = init(4),
    start(4),
    {ok, 1} = request(),
    {ok, 2} = request(),
    {ok, 3} = request(),
    {ok, 4} = request(),
    {error, no_key} = request(),
    ok = return(2),
    {ok, 2} = request(),
    ok.
