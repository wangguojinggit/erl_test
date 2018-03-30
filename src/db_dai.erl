-module(db).
-compile(export_all).

start() ->
    register(db, spawn(fun() -> loop({[], []}) end)).

new({private, Table}) ->
    db ! {self(), new, {private, self(), Table}},
    receive Rsp -> Rsp end;
new(Table) ->
    db ! {self(), new, {public, Table}},
    receive Rsp -> Rsp end.

read({private, Table}, Key) ->
    db ! {self(), read, {self(), Table}, Key},
    receive Rsp -> Rsp end;
read(Table, Key) ->
    db ! {self(), read, Table, Key},
    receive Rsp -> Rsp end.

del({private, Table}, Key) ->
    db ! {self(), delete, {self(), Table}, Key},
    receive Rsp -> Rsp end;
del(Table, Key) ->
    db ! {self(), delete, Table, Key},
    receive Rsp -> Rsp end.

insert({private, Table}, Key, Val) ->
    db ! {self(), insert, {self(), Table}, Key, Val},
    receive Rsp -> Rsp end;
insert(Table, Key, Val) ->
    db ! {self(), insert, Table, Key, Val},
    receive Rsp -> Rsp end.

loop(State = {PubTables, PrivTableList}) ->
    receive
        {Sender, new, {public, Table}} ->
            case create_table(Table, PubTables) of
                {ok, NewPubTables} ->
                    Sender ! create_table_ok,
                    loop({NewPubTables, PrivTableList});
                {fail, Res} ->
                    Sender ! Res,
                    loop(State)
            end;
        {Sender, new, {private, Owner, Table}} ->
            case create_private_table(Owner, Table, PrivTableList) of
                {ok, NewPrivTableList} ->
                    Sender ! create_table_ok,
                    loop({PubTables, NewPrivTableList});
                {fail, Res} ->
                    Sender ! Res,
                    loop(State)
            end;
        {Sender, read, Table, Key} ->
            TabPid = get_table_pid(Table, {PubTables, PrivTableList}),
            Sender ! read_record(TabPid, Key),
            loop(State);
        {Sender, delete, Table, Key} ->
            TabPid = get_table_pid(Table, {PubTables, PrivTableList}),
            Sender ! delete_record(TabPid, Key),
            loop(State);
        {Sender, insert, Table, Key, Val} ->
            TabPid = get_table_pid(Table, {PubTables, PrivTableList}),
            io:format("Table:~p, TabPid:~p~n", [Table, TabPid]),
            Sender ! insert_record(TabPid, Key, Val),
            loop(State);
        stop ->
            stop
    end.

create_table(Table, PubTables) ->
    case find(Table, PubTables) of
        not_found ->
            Pid = spawn(fun() -> table_loop([]) end),
            {ok, [{Table, Pid} | PubTables]};
        _ ->
            {fail, table_existed}
    end.

create_private_table(Owner, Table, PrivTableList) ->
    case find(Owner, PrivTableList) of
        not_found ->
            Pid = spawn(fun() -> table_loop([]) end),
            {ok, [{Owner, [{Table, Pid}]} | PrivTableList]};
        OwnerTables ->
%%            io:format("Owner:~p, Table:~p, Tables:~p~n", [Owner, Table, OwnerTables]),
            case find(Table, OwnerTables) of
                not_found ->
                    Pid = spawn(fun() -> table_loop([]) end),
                    NewPrivTableList = replace(Owner, [{Table, Pid} | OwnerTables], PrivTableList),
                    {ok, NewPrivTableList};
                _ ->
                    {fail, table_existed}
            end
    end.

get_table_pid({Owner, Table}, {_PubTables, PrivTableList}) ->
    case find(Owner, PrivTableList) of
        not_found -> not_found;
        OwnerTables ->
            find(Table, OwnerTables)
    end;
get_table_pid(PubTable, {PubTables, _PrivTableList}) ->
    find(PubTable, PubTables).

read_record(not_found, _Key) -> no_table;
read_record(Pid, Key) ->
    Pid ! {self(), read, Key},
    receive
        Res -> Res
    end.

delete_record(not_found, _Key) -> no_table;
delete_record(Pid, Key) ->
    Pid ! {self(), delete, Key},
    receive
        Res -> Res
    end.

insert_record(not_found, _Key, _Val) -> no_table;
insert_record(Pid, Key, Val) ->
    Pid ! {self(), insert, Key, Val},
    receive
        Res -> Res
    end.


table_loop(Datas) ->
    receive
        {Sender, insert, Key, Val} ->
            case find(Key, Datas) of
                not_found ->
                    Sender ! insert_ok,
                    table_loop([{Key, Val} | Datas]);
                _ ->
                    Sender ! insert_ok,
                    table_loop(Datas)
            end;
        {Sender, read, Key} ->
            Sender ! find(Key, Datas),
            table_loop(Datas);
        {Sender, delete, Key} ->
            Sender ! delete_ok,
            table_loop(delete(Key, Datas));
        stop -> stop
    end.


find(_Key, []) -> not_found;
find(Key, [{Key, Val} | _Rest]) -> Val;
find(Key, [_ | Rest]) -> find(Key, Rest).

replace(_Key, _Val, []) -> [];
replace(Key, Val, [{Key, _} | Rest]) -> [{Key, Val} | Rest];
replace(Key, Val, [Head | Rest]) -> [Head | replace(Key, Val, Rest)].

delete(_, []) -> [];
delete(Key, [{Key,_} | Rest]) -> Rest;
delete(Key, [Head | Rest]) -> [Head | delete(Key, Rest)].
