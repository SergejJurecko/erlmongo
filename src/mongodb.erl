-module(mongodb).
-export([print_info/0, start/0, stop/0, init/1, handle_call/3,
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
% API
-export([connect/1, connect/2, is_connected/1,deleteConnection/1,
		 singleServer/2, singleServer/3, singleServer/1, singleServer/5,
		 masterSlave/3, masterSlave/4, masterSlave/6,
		 masterMaster/3, masterMaster/4, masterMaster/6,
		 replicaPairs/3, replicaPairs/4, replicaPairs/6,
		 datetime_to_now/1,
		 replicaSets/2, replicaSets/3, replicaSets/5,
		 sharded/2, sharded/3, sharded/5]).
% Internal
-export([exec_cursor/3, exec_delete/3, exec_cmd/3, exec_insert/3, exec_find/3, exec_update/3, exec_getmore/3,
          ensureIndex/3, clearIndexCache/0, create_id/0, startgfs/1]).
-include_lib("erlmongo.hrl").
% -compile(export_all).

-define(MONGO_PORT, 27017).
-define(RECONNECT_DELAY, 1000).

-define(OP_REPLY, 1).
-define(OP_MSG, 1000).
-define(OP_UPDATE, 2001).
-define(OP_INSERT, 2002).
-define(OP_QUERY, 2004).
-define(OP_GET_MORE, 2005).
-define(OP_DELETE, 2006).
-define(OP_KILL_CURSORS, 2007).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

% register() ->
% 	supervisor:start_child(supervisor, {?MODULE, {?MODULE, start, []}, permanent, 1000, worker, [?MODULE]}).

print_info() ->
	gen_server:cast(?MODULE, {print_info}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%								API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect(Pool) when is_atom(Pool) ->
	gen_server:cast(?MODULE, {start_connection, Pool, undefined}).
% For when connection is established. Parameter can be:
% - {Module,Function,Params}
% - PID, that gets a {mongodb_connected} message
connect(Pool, Callback) when is_atom(Pool) andalso
		(is_pid(Callback) orelse is_tuple(Callback) andalso tuple_size(Callback) == 3) ->
	gen_server:cast(?MODULE, {start_connection, Pool, Callback}).

deleteConnection(Pool) when is_atom(Pool) ->
	gen_server:cast(?MODULE,{delete_connection,Pool}).

is_connected(Pool) when is_atom(Pool) ->
	Sz = ets:info(Pool,size),
	is_integer(Sz) andalso Sz > 0.

singleServer(Pool) ->
	singleServer(Pool, 10).
singleServer(Pool, Size) when is_atom(Pool), is_integer(Size) ->
	singleServer(Pool, Size, "localhost:"++integer_to_list(?MONGO_PORT)).
singleServer(Pool, Size, [_|_] = Addr) when is_atom(Pool), is_integer(Size) ->
	singleServer(Pool, Size, Addr, undefined, undefined).
singleServer(Pool, Size, [_|_] = Addr, Username, Pw) when is_atom(Pool), is_integer(Size) ->
	[IP,Port] = parse_addr(Addr),
	gen_server:cast(?MODULE, {conninfo, Pool, Size, Username, Pw, [{IP,Port}, {IP,Port}]}).

masterSlave(Pool, M, S) ->
	masterSlave(Pool, 10, M, S).
masterSlave(Pool, Size, [_|_] = MasterAddr, [_|_] = SlaveAddr) ->
	masterSlave(Pool, Size, MasterAddr, SlaveAddr, undefine, undefined).
masterSlave(Pool, Size, [_|_] = MasterAddr, [_|_] = SlaveAddr, Username, Pw) when is_atom(Pool), is_integer(Size) ->
	[IP1,Port1] = parse_addr(MasterAddr),
	[IP2,Port2] = parse_addr(SlaveAddr),
	gen_server:cast(?MODULE, {conninfo,Pool, Size, Username, Pw, [{IP1,Port1}, {IP2,Port2}]}).
	% gen_server:cast(?MODULE, {conninfo,Pool, Size, {masterSlave, {IP1,Port1}, {IP2,Port2}}}).

masterMaster(Pool, A1, A2) ->
	masterMaster(Pool, 10, A1, A2).
masterMaster(Pool,Size,[_|_] = Addr1, [_|_] = Addr2) ->
	sharded(Pool,Size,[Addr1,Addr2], undefined, undefined).
masterMaster(Pool, Size, A1, A2, Us, Pw) ->
	sharded(Pool, Size, [A1, A2], Us, Pw).

sharded(Pool, L) ->
	sharded(Pool, 10, L).
sharded(Pool, Size, L) ->
	sharded(Pool, Size, L, undefined, undefined).
sharded(Pool, Size, [[_|_]|_] = L, Us, Pw) when is_atom(Pool), is_integer(Size) ->
	SL = [list_to_tuple(parse_addr(A)) || A <- L],
	gen_server:cast(?MODULE, {conninfo,Pool, Size, Us, Pw, SL});
sharded(Pool, Size, [_|_] = L, Us, Pw) ->
	sharded(Pool, Size,[L], Us, Pw).

replicaPairs(Pool, A1, A2) ->
	replicaPairs(Pool, 10, A1, A2).
replicaPairs(Pool, Size, [_|_] = Addr1, [_|_] = Addr2) ->
	replicaPairs(Pool, Size, Addr1, Addr2, undefined, undefined).
replicaPairs(Pool, Size, [_|_] = Addr1, [_|_] = Addr2, Us, Pw) when is_atom(Pool) ->
	[IP1,Port1] = parse_addr(Addr1),
	[IP2,Port2] = parse_addr(Addr2),
	% gen_server:cast(?MODULE, {conninfo,Pool, Size, {replicaPairs, {IP1,Port1}, {IP2,Port2}}}).
	gen_server:cast(?MODULE, {conninfo,Pool, Size, Us, Pw, [{IP1,Port1}, {IP2,Port2}]}).
% Takes a list of "Address:Port"
replicaSets(Pool,L) ->
	replicaSets(Pool,10,L).
replicaSets(Pool,Size,L) ->
	replicaSets(Pool, Size, L, undefined, undefined).
replicaSets(Pool,Size,L, Us, Pw) when is_atom(Pool), is_integer(Size) ->
	LT = [list_to_tuple(parse_addr(S)) || S <- L],
	% gen_server:cast(?MODULE,{conninfo,Pool,Size,{replicaSets,LT}}).
	gen_server:cast(?MODULE,{conninfo,Pool,Size,Us,Pw,LT}).

parse_addr(A) ->
	case string:tokens(A,":") of
		[IP,Port] ->
			[IP,Port];
		[IP] ->
			[IP, integer_to_list(?MONGO_PORT)]
	end.

datetime_to_now(Loctime) ->
	Secs = calendar:datetime_to_gregorian_seconds(Loctime) - 719528 * 24 * 60 * 60,
	{Secs div 1000000, Secs rem 1000000,0}.

ensureIndex(Pool,DB,Bin) ->
	gen_server:cast(?MODULE, {ensure_index,Pool, DB, Bin}).
clearIndexCache() ->
	gen_server:cast(?MODULE, {clear_indexcache}).

exec_cursor(Pool,Col, Quer) ->
	case trysend(Pool,{find, self(), Col, Quer}, safe) of
		{ok,MonRef,Pid} ->
			receive
				{'DOWN', _MonitorRef, _, Pid, _Why} ->
					not_connected;
				{query_result, _Src, <<_:32,CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
					erlang:demonitor(MonRef),
					% io:format("cursor ~p from ~p ndocs ~p, ressize ~p ~n", [_CursorID, _From, _NDocs, byte_size(Result)]),
					% io:format("~p~n", [Result]),
					case CursorID of
						0 ->
							{done, Result};
						_ ->
							PIDcl = spawn_link(fun() -> cursorcleanup(Pool) end),
							PIDcl ! {start, CursorID},
							{#cursor{id = CursorID, limit = Quer#search.ndocs, pid = PIDcl}, Result}
					end
				after 5000 ->
					not_connected
			end;
		X ->
			X
	end.
exec_getmore(Pool,Col, C) ->
	case erlang:is_process_alive(C#cursor.pid) of
		false ->
			{done, <<>>};
		true ->
			case trysend(Pool,{getmore, self(), Col, C},safe) of
				{ok,MonRef,Pid} ->
					receive
						{'DOWN', _MonitorRef, _, Pid, _Why} ->
							not_connected;
						{query_result, _Src, <<_:32,CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
							erlang:demonitor(MonRef),
							% io:format("cursor ~p from ~p ndocs ~p, ressize ~p ~n", [_CursorID, _From, _NDocs, byte_size(Result)]),
							% io:format("~p~n", [Result]),
							case CursorID of
								0 ->
									C#cursor.pid ! {stop},
									{done, Result};
								_ ->
									{ok, Result}
							end
						after 5000 ->
							erlang:demonitor(MonRef),
							{done, <<>>}
					end;
				X ->
					X
			end
	end.
exec_delete(Pool,Collection, D) ->
	trysend(Pool,{delete,Collection,D},unsafe).

exec_find(Pool,Collection, Quer) ->
	case trysend(Pool,{find, self(), Collection, Quer}, safe) of
		{ok,MonRef,Pid} ->
			receive
				{'DOWN', _MonitorRef, _, Pid, _Why} ->
					timer:sleep(10),
					exec_find(Pool, Collection, Quer);
				{query_result, Pid, <<_:32,_CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
					erlang:demonitor(MonRef),
					Result
				after 200000 ->
					erlang:demonitor(MonRef),
					Pid ! {forget,self()},
					not_connected
			end;
		X ->
			X
	end.
exec_insert(Pool,Collection, D) ->
	trysend(Pool,{insert,Collection,D}, unsafe).
exec_update(Pool,Collection, D) ->
	trysend(Pool,{update,Collection,D}, unsafe).
exec_cmd(Pool,DB, Cmd) ->
	Quer = #search{ndocs = 1, nskip = 0, criteria = bson:encode(Cmd)},
	case exec_find(Pool,<<DB/binary, ".$cmd">>, Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			[];
		Result ->
			case bson:decode(Result) of
				[Res] ->
					Res;
				Res ->
					Res
			end
	end.

trysend(Pool,Query,Type) when is_atom(Pool) ->
	Sz = ets:info(Pool,size),
	case Sz of
		0 ->
			not_connected;
		_ ->
			Pos = rand:uniform(Sz)-1,
			case (catch ets:slot(Pool,Pos)) of
				[{Pid}] ->
					trysend(Pid,Query,Type);
				_ ->
					case ets:first(Pool) of
						'$end_of_table' ->
							not_connected;
						Pid when is_pid(Pid) ->
							trysend(Pid,Query,Type)
					end
			end
	end;
trysend(Pid,Query,safe) ->
	MonRef = erlang:monitor(process,Pid),
	Pid ! Query,
	{ok,MonRef,Pid};
trysend(Pid,Query,unsafe) ->
	Pid ! Query,
	ok.

create_id() ->
	bson:dec2hex(<<>>, gen_server:call(?MODULE, {create_oid})).

startgfs(P) ->
	PID = spawn_link(fun() -> gfs_proc(P,<<>>) end),
	PID ! {start},
	PID.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%								IMPLEMENTATION
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(STATE_INIT, init).
-define(STATE_ACTIVE, active).
-define(STATE_CLOSING, closing).

% read = connection used for reading (find) from mongo server
% write = connection used for writing (insert,update) to mongo server
%   single: same as replicaPairs (single server is always master and used for read and write)
%   masterSlave: read = write = master
%   replicaPairs: read = write = master
%   masterMaster: pick one at random
% timer is reconnect timer if some connection is missing
-record(pool, {name, size = 0, info, cb, style=default, us, pw}).
% indexes is ensureIndex cache (an ets table).
% pids: #{PID => {PoolName,[active|init|closing]}}
% retry: [PoolName1,PoolName2]
-record(mngd, {indexes, pools = [], pids = #{}, retry = [], hashed_hostn, oid_index = 1}).

handle_call({create_oid}, _, P) ->
	WC = element(1,erlang:statistics(wall_clock)) rem 16#ffffffff,
	% <<_:20/binary,PID:2/binary,_/binary>> = term_to_binary(self()),
	N = P#mngd.oid_index rem 16#ffffff,
	Out = <<WC:32, (P#mngd.hashed_hostn)/binary, (list_to_integer(os:getpid())):16, N:24>>,
	{reply, Out, P#mngd{oid_index = P#mngd.oid_index + 1}};
% handle_call({is_connected,Name}, _, P) ->
% 	case get(Name) of
% 		X when is_pid(X#conn.pid) ->
% 			{reply, true, P};
% 		_X ->
% 			{reply, false, P}
% 	end;
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call(_, _, P) ->
	{reply, ok, P}.

handle_cast({ensure_index,Pool, DB, Bin}, P) ->
	case ets:lookup(P#mngd.indexes, {DB,Bin}) of
		[] ->
			spawn(fun() ->
				Mon = mongoapi:new(Pool,DB),
				case Mon:insert(<<"system.indexes">>, hd(bson:decode(Bin))) of
					ok ->
						gen_server:cast(?MODULE,{ensure_index_store, DB, Bin});
					{error,[Ok|_]} when Ok > 0 ->
						gen_server:cast(?MODULE,{ensure_index_store, DB, Bin});
					_E ->
						error_logger:error_msg("erlmongo ensure_index failed db=~p, err=~p~n", [DB,_E]),
						ok
				end
			end);
		_ ->
			true
	end,
	{noreply, P};
handle_cast({ensure_index_store, DB,Bin}, P) ->
	ets:insert(P#mngd.indexes, {{DB,Bin}}),
	{noreply, P};
handle_cast({clear_indexcache}, P) ->
	ets:delete_all_objects(P#mngd.indexes),
	{noreply, P};
handle_cast({conninfo, Pool, Size, Us, Pw, Info}, P) ->
	?DBG("conninfo ~p", [{Pool,Size,Info}]),
	Pools = case lists:keyfind(Pool,#pool.name,P#mngd.pools) of
		false ->
			ets:new(Pool,[named_table,public,set]),
			[#pool{name = Pool, size = Size, info = Info, us = Us, pw = Pw}|P#mngd.pools];
		#pool{info = OldInfo} when OldInfo == Info ->
			P#mngd.pools;
		Existing ->
			lists:keystore(Pool, 1, P#mngd.pools, Existing#pool{info = Info})
	end,
	handle_cast(save_connections,P#mngd{pools = Pools});
handle_cast(save_connections,P) ->
	L = [{Pool#pool.name, Pool#pool.size, Pool#pool.info, Pool#pool.us, Pool#pool.pw} || Pool <- P#mngd.pools],
	application:set_env(erlmongo,connections,L),
	{noreply, P};
handle_cast({start_connection, Pool,CB}, P) ->
	?DBG("start_connection ~p ~p~n", [Pool, P#mngd.pools]),
	case lists:keyfind(Pool, #pool.name, P#mngd.pools) of
		false ->
			{noreply, P};
		PI ->
			{noreply, start_connection(P, Pool, PI#pool{cb = CB})}
	end;
handle_cast({delete_connection, Pool}, P) ->
	case lists:keymember(Pool,#pool.name, P#mngd.pools) of
		false ->
			{noreply, P};
		_ ->
		[Pid ! stop || {Pid} <- ets:tab2list(Pool)],
		ets:delete(Pool),
		handle_cast(save_connections, P#mngd{pools = lists:keydelete(Pool,#pool.name,P#mngd.pools)})
	end;
handle_cast({print_info}, P) ->
	io:format("~p ~p~n~p~n", [self(),get(),P]),
	{noreply, P};
handle_cast(_, P) ->
	{noreply, P}.

startcon(Name, Addr, Port, Us, Pw) when is_list(Port) ->
	startcon(Name, Addr, list_to_integer(Port), Us, Pw);
startcon(Name, Addr, Port, Us, Pw) ->
	{PID,_} = spawn_monitor(fun() -> connection(true) end),
	PID ! {start, Name, self(), Addr, Port, Us, Pw},
	PID.

start_connection(P, PoolName, PI) ->
	Servers = PI#pool.info,
	Existing = [PID || {PID,{Pool1,State}} <- maps:to_list(P#mngd.pids),
		Pool1 == PoolName andalso (State == ?STATE_ACTIVE orelse State == ?STATE_INIT)],
	ExLen = length(Existing),
	LenServers = length(Servers),
	case ok of
		_ when ExLen < PI#pool.size ->
			Additional = [begin
					{IP,Port} = lists:nth(rand:uniform(LenServers), Servers),
					PID = startcon(PoolName, IP, Port, PI#pool.us, PI#pool.pw),
					{PID, {PoolName,?STATE_INIT}}
				end	|| _ <- lists:seq(1,PI#pool.size - ExLen)],
			P#mngd{pids = maps:merge(P#mngd.pids, maps:from_list(Additional))};
		_ when ExLen > PI#pool.size ->
			{_Keepers,Gonners} = lists:split(PI#pool.size, Existing),
			[begin
				PID ! stop,
				PID
			end || PID <- Gonners],
			New = maps:fold(fun(PID,_,Map) ->
				case lists:member(PID,Gonners) of
					true ->
						ets:delete(PoolName, PID),
						maps:put(PID,{PoolName,?STATE_CLOSING}, Map);
					false ->
						Map
				end
			end, P#mngd.pids,P#mngd.pids),
			P#mngd{pids = New};
		_ ->
			P
	end.

conn_callback(P) ->
	case is_pid(P) of
		true ->
			P ! {mongodb_connected};
		false ->
			case P of
				{Mod,Fun,Param} ->
					spawn(Mod,Fun,Param);
				_ ->
					true
			end
	end.

reconnect(#mngd{retry = [H|T]} = P) ->
	case lists:keyfind(H,#pool.name, P#mngd.pools) of
		false ->
			reconnect(P#mngd{retry = T});
		PI ->
			reconnect(start_connection(P#mngd{retry = T}, H, PI))
	end;
reconnect(P) ->
	P.

handle_info(reconnect, P) ->
	% case P#mngd.retry of
	% 	[_|_] ->
	% 		io:format("reconnect ~p~n",[P#mngd.retry]);
	% 	_ ->
	% 		ok
	% end,
	erlang:send_after(?RECONNECT_DELAY, self(),reconnect),
	{noreply, reconnect(P)};
handle_info({'DOWN', _MonitorRef, _, PID, Why}, P) ->
	?DBG("conndied ~p ~p", [PID,maps:get(PID,P#mngd.pids, undefined)]),
	% io:format("condied ~p~n", [{PID,Why}]),
	case maps:get(PID,P#mngd.pids, undefined) of
		undefined ->
			{noreply, P};
		{Pool,?STATE_INIT} ->
			{noreply, P#mngd{pids = maps:remove(PID,P#mngd.pids), retry = add(Pool,P#mngd.retry)}};
		{Pool,?STATE_CLOSING} ->
			ets:delete(Pool, PID),
			{noreply, P#mngd{pids = maps:remove(PID,P#mngd.pids)}};
		{Pool,?STATE_ACTIVE} ->
			ets:delete(Pool, PID),
			error_logger:error_msg("erlmongo connection died ~p reason=~p pool=~p~n", [PID,Why,Pool]),
			handle_cast({start_connection, Pool, undefined}, P#mngd{pids = maps:remove(PID,P#mngd.pids)})
	end;
handle_info({query_result, Src, <<_:20/binary, Res/binary>>}, P) ->
	case maps:get(Src, P#mngd.pids, undefined) of
		{Pool,PidState} ->
			% io:format("got ismaster query_result ~p~n", [Pool]),
			case catch bson:decode(Res) of
				[Obj] ->
					case proplists:get_value(<<"ismaster">>,Obj) of
						Val when Val == true; Val == 1 ->
							?DBG("foundmaster ~p~n", [Src]),
							case is_connected(Pool) of
								false ->
									ets:insert(Pool,{Src}),
									PI = lists:keyfind(Pool,#pool.name, P#mngd.pools),
									conn_callback(PI#pool.cb);
								true when PidState == ?STATE_INIT ->
									ets:insert(Pool,{Src});
								true ->
									ok
							end,
							case PidState of
								?STATE_INIT ->
									{noreply, P#mngd{pids = maps:put(Src,{Pool,active}, P#mngd.pids)}};
								?STATE_ACTIVE ->
									{noreply, P};
								_ ->
									{noreply, P}
							end;
						_  ->
							Src ! {stop},
							case proplists:get_value(<<"primary">>,Obj) of
								undefined ->
									{noreply, P#mngd{pids = maps:remove(Src,P#mngd.pids), retry = add(Pool,P#mngd.retry)}};
								Prim ->
									?DBG("Connecting to primary ~p", [Prim]),
									[Addr,Port] = string:tokens(binary_to_list(Prim),":"),
									PI = lists:keyfind(Pool,#pool.name, P#mngd.pools),
									PID = startcon(Pool, Addr,Port, PI#pool.us, PI#pool.pw),
									{noreply, P#mngd{pids = maps:remove(Src,maps:put(PID,{Pool,init},P#mngd.pids))}}
							end
					end;
				_ ->
					Src ! {stop},
					{noreply, P#mngd{pids = maps:remove(Src,P#mngd.pids), retry = add(Pool,P#mngd.retry)}}
			end;
		undefined ->
			Src ! {stop},
			{noreply, P}
	end;
handle_info({query_result, Src, _}, P) ->
	Src ! {stop},
	{Pool,?STATE_INIT} = maps:get(Src, P#mngd.pids),
	error_logger:error_msg("erlmongo ismaster invalid response pool=~p", [Pool]),
	{noreply, P#mngd{pids = maps:remove(Src,P#mngd.pids), retry = add(Pool,P#mngd.retry)}};
handle_info(stop,_P) ->
	exit(stop);
handle_info(_X, P) ->
	io:format("~p~n", [_X]),
	{noreply, P}.

add(K,L) ->
	case lists:member(K,L) of
		true ->
			L;
		false ->
			[K|L]
	end.

% conndied(Name,PID,P) when P#conn.pid == PID ->
% 	put(Name, P#conn{pid = undefined, timer = timer(P#conn.timer, Name)});
% conndied(_,_,_) ->
% 	ok.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	erlang:send_after(?RECONNECT_DELAY, self(),reconnect),
	case application:get_env(erlmongo,connections) of
		{ok, L} ->
			[gen_server:cast(?MODULE,{conninfo, Pool, Sz, Info, Us, Pw}) || {Pool,Sz,Info,Us,Pw} <- L],
			[connect(Pool) || {Pool,_,_} <- L];
		_ ->
			true
	end,
	{ok, HN} = inet:gethostname(),
	<<HashedHN:3/binary,_/binary>> = erlang:md5(HN),
	{ok, #mngd{indexes = ets:new(mongoIndexes, [set, private]), hashed_hostn = HashedHN}}.



gfs_proc(#gfs_state{mode = write} = P, Buf) ->
	receive
		{write, Bin} ->
			Compl = <<Buf/binary, Bin/binary>>,
			case true of
				_ when byte_size(Compl) >= P#gfs_state.flush_limit ->
					self() ! {flush};
				_ ->
					true
			end,
			gfs_proc(P, Compl);
		{flush} ->
			FlSize = (byte_size(Buf) div (P#gfs_state.file)#gfs_file.chunkSize) * (P#gfs_state.file)#gfs_file.chunkSize,
			<<FlushBin:FlSize/binary,Rem/binary>> = Buf,
			gfs_proc(gfsflush(P, FlushBin, <<>>),Rem);
		{close} ->
			gfsflush(P#gfs_state{closed = true}, Buf, <<>>);
		{'EXIT',_,_} ->
			self() ! {close},
			gfs_proc(P,Buf);
		{start} ->
			process_flag(trap_exit,true),
			FileID = (P#gfs_state.file)#gfs_file.docid,
			exec_update(P#gfs_state.pool,<<(P#gfs_state.collection)/binary, ".files">>, #update{selector = bson:encode([{<<"_id">>, {oid, FileID}}]),
																		document = bson:encoderec(P#gfs_state.file)}),
			Keys = [{<<"files_id">>, 1},{<<"n">>,1}],
			Bin = bson:encode([{plaintext, <<"name">>, bson:gen_prop_keyname(Keys, <<>>)},
			 					  {plaintext, <<"ns">>, <<(P#gfs_state.collection)/binary, ".chunks">>},
			                      {<<"key">>, {bson, bson:encode(Keys)}}]),
			ensureIndex(P#gfs_state.pool,P#gfs_state.db, Bin),
			gfs_proc(P,<<>>)
		% X ->
		% 	io:format("Received unknown msg ~p~n", [X])
	end;
gfs_proc(#gfs_state{mode = read} = P, Buf) ->
	receive
		{read, Source, RecN} ->
			CSize = (P#gfs_state.file)#gfs_file.chunkSize,
			FileLen = (P#gfs_state.file)#gfs_file.length,
			case FileLen - CSize * P#gfs_state.nchunk of
				LenRem when LenRem >= RecN ->
					N = RecN;
				LenRem when LenRem > 0 ->
					N = LenRem;
				_ ->
					N = byte_size(Buf)
			end,
			case true of
				_ when N =< byte_size(Buf) ->
					<<Ret:N/binary, Rem/binary>> = Buf,
					Source ! {gfs_bytes, Ret},
					gfs_proc(P, Rem);
				_ ->
					GetChunks = ((N - byte_size(Buf)) div CSize) + 1,
					Quer = #search{ndocs = GetChunks, nskip = 0,
								   criteria = bson:encode([{<<"files_id">>, (P#gfs_state.file)#gfs_file.docid},
															  {<<"n">>, {in,{gte, P#gfs_state.nchunk},{lte, P#gfs_state.nchunk + GetChunks}}}]),
								   field_selector = get(field_selector)},
					case exec_find(P#gfs_state.pool,<<(P#gfs_state.collection)/binary, ".chunks">>, Quer) of
						not_connected ->
							Source ! not_connected,
							gfs_proc(P,Buf);
						<<>> ->
							Source ! eof,
							gfs_proc(P,Buf);
						ResBin ->
							% io:format("Result ~p~n", [ResBin]),
							Result = chunk2bin(bson:decode(ResBin), <<>>),
							case true of
								_ when byte_size(Result) + byte_size(Buf) =< N ->
									Rem = <<>>,
									Source ! {gfs_bytes, <<Buf/binary, Result/binary>>};
								_ ->
									<<ReplyBin:N/binary, Rem/binary>> = <<Buf/binary, Result/binary>>,
									Source ! {gfs_bytes, ReplyBin}
							end,
							gfs_proc(P#gfs_state{nchunk = P#gfs_state.nchunk + GetChunks}, Rem)
					end
			end;
		{close} ->
			true;
		{start} ->
			put(field_selector, bson:encode([{<<"data">>, 1}])),
			gfs_proc(P, <<>>)
	end.

chunk2bin([[_, {_, {binary, 2, Chunk}}]|T], Bin) ->
	chunk2bin(T, <<Bin/binary, Chunk/binary>>);
chunk2bin(_, B) ->
	B.


gfsflush(P, Bin, Out) ->
	CSize = (P#gfs_state.file)#gfs_file.chunkSize,
	FileID = (P#gfs_state.file)#gfs_file.docid,
	case Bin of
		<<ChunkBin:CSize/binary, Rem/binary>> ->
			Chunk = #gfs_chunk{docid = {oid,create_id()}, files_id = FileID, n = P#gfs_state.nchunk, data = {binary, 2, ChunkBin}},
			gfsflush(P#gfs_state{nchunk = P#gfs_state.nchunk + 1, length = P#gfs_state.length + CSize},
					 Rem, <<Out/binary, (bson:encoderec(Chunk))/binary>>);
		Rem when P#gfs_state.closed == true, byte_size(Rem) > 0 ->
			Chunk = #gfs_chunk{docid = {oid,create_id()}, files_id = FileID, n = P#gfs_state.nchunk, data = {binary, 2, Rem}},
			gfsflush(P#gfs_state{length = P#gfs_state.length + byte_size(Rem)},
			         <<>>, <<Out/binary, (bson:encoderec(Chunk))/binary>>);
		Rem when byte_size(Out) > 0 ->
			File = P#gfs_state.file,
			exec_insert(P#gfs_state.pool,<<(P#gfs_state.collection)/binary, ".chunks">>, #insert{documents = Out}),
			case P#gfs_state.closed of
				true ->
					MD5Cmd = exec_cmd(P#gfs_state.pool,P#gfs_state.db, [{<<"filemd5">>, FileID},{<<"root">>, P#gfs_state.coll_name}]),
					case proplists:get_value(<<"md5">>,MD5Cmd) of
						undefined ->
							error_logger:error_msg("Md5 cmd failed ~p", [MD5Cmd]),
							MD5 = undefined,
							ok;
						MD5 ->
							ok
					end;
				false ->
					MD5 = undefined
			end,
			exec_update(P#gfs_state.pool,<<(P#gfs_state.collection)/binary, ".files">>, #update{selector = bson:encode([{<<"_id">>, FileID}]),
																		document = bson:encoderec(File#gfs_file{length = P#gfs_state.length,
																		                                           md5 = MD5})}),
			gfsflush(P, Rem, <<>>);
		_Rem ->
			P
	end.

-record(ccd, {conn,cursor = 0}).
% Just for cleanup
cursorcleanup(P) ->
	receive
		{stop} ->
			true;
		{cleanup} ->
			P#ccd.conn ! {killcursor, #killc{cur_ids = <<(P#ccd.cursor):64/little>>}};
		{'EXIT', _PID, _Why} ->
			self() ! {cleanup},
			cursorcleanup(P);
		{start, Cursor} ->
			process_flag(trap_exit, true),
			cursorcleanup(#ccd{conn = P,cursor = Cursor})
	end.


-record(con, {sock, die = false, die_attempt_cnt = 0, auth}).
-record(auth, {step = 0, us, pw, source, nonce, first_msg, sig, conv_id}).
con_candie() ->
	[Pid || {Ind,Pid} <- get(), is_integer(Ind) andalso is_pid(Pid)] == [].
% Proc. d.:
%   {ReqID, ReplyPID}
% Waiting for request
connection(_) ->
	connection(#con{},1,<<>>).
connection(#con{} = P,Index,Buf) ->
	receive
		{forget, Source} ->
			case lists:keyfind(Source,2,get()) of
				false ->
					ok;
				Id ->
					erase(Id)
			end,
			connection(P,Index,Buf);
		{find, Source, Collection, Query} ->
			% io:format("Q ~p~n", [{get(),Index,Source}]),
			QBin = constr_query(Query,Index, Collection),
			ok = gen_tcp:send(P#con.sock, QBin),
			put(Index,Source),
			connection(P, Index+1, Buf);
		{insert, Collection, Doc} ->
			Bin = constr_insert(Doc, Collection),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P, Index,Buf);
		{update, Collection, #update{} = Doc} ->
			Bin = constr_update(Doc, Collection),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P,Index, Buf);
		{update, Collection, [_|_] = Doc} ->
			Bin = lists:foldl(fun(D,B) -> <<B/binary,(constr_update(D, Collection))/binary>> end, <<>>,Doc),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P,Index, Buf);
		{delete, Col, D} ->
			Bin = constr_delete(D, Col),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P,Index, Buf);
		{getmore, Source, Col, C} ->
			Bin = constr_getmore(C, Index, Col),
			ok = gen_tcp:send(P#con.sock, Bin),
			put(Index,Source),
			connection(P,Index+1, Buf);
		{killcursor, C} ->
			Bin = constr_killcursors(C),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P,Index, Buf);
		{tcp, _, Bin} ->
			% io:format("~p~n", [{byte_size(Bin), Buf}]),
			connection(P,Index,readpacket(<<Buf/binary,Bin/binary>>));
		{ping} ->
			erlang:send_after(1000,self(),{ping}),
			self() ! {find, whereis(?MODULE), <<"admin.$cmd">>,
				#search{nskip = 0, ndocs = 1, criteria = bson:encode([{<<"ismaster">>, 1}])}},
			connection(P, Index, Buf);
			% Collection = <<"admin.$cmd">>,
			% Query = #search{nskip = 0, ndocs = 1, criteria = bson:encode([{<<"ping">>, 1}])},
			% QBin = constr_query(Query,Index, Collection),
			% ok = gen_tcp:send(P#con.sock, QBin),
			% connection(P,Index+1,Buf);
		{stop} ->
			case con_candie() of
				true ->
					ok;
				_ ->
					connection(P#con{die = true})
			end;
		{start, _Pool, Source, IP, Port, Us, Pw} ->
			{ok, Sock} = gen_tcp:connect(IP, Port, [binary, {packet, 0}, {active, true}, {keepalive, true}], 1000),
			erlang:send_after(1000,self(),{ping}),
			connection(#con{sock = Sock, auth = init_auth(Source, Us, Pw)},1, <<>>);
		{query_result, _Me, <<_:32,_CursorID:64/little, _From:32/little, _NDocs:32/little, Packet/binary>>} ->
			connection(P#con{auth = scram_step(P#con.auth, Packet)},Index, Buf);
		{tcp_closed, _} ->
			exit(tcp_closed)
		after 5000 ->
			case P#con.die of
				true ->
					case con_candie() of
						true ->
							ok;
						_ when P#con.die_attempt_cnt > 2 ->
							ok;
						_ ->
							connection(P#con{die_attempt_cnt = P#con.die_attempt_cnt + 1})
					end;
				false ->
					connection(P)
			end
	end.

init_auth(Source, undefined,undefined) ->
	self() ! {find, Source, <<"admin.$cmd">>,
		#search{nskip = 0, ndocs = 1, criteria = bson:encode([{<<"ismaster">>, 1}])}},
	undefined;
init_auth(Source, Us,Pw) ->
	scram_first_step_start(#auth{us = Us, pw = Pw, source = Source}).

readpacket(<<ComplSize:32/little, _ReqID:32/little,RespID:32/little,_OpCode:32/little, Body/binary>> = Bin) ->
	BodySize = ComplSize-16,
	case Body of
		<<Packet:BodySize/binary,Rem/binary>> ->
			case is_pid(get(RespID)) of
				true ->
					get(RespID) ! {query_result, self(), Packet},
					erase(RespID);
				false ->
					true
			end,
			case Rem of
				<<>> ->
					<<>>;
				_ ->
					readpacket(Rem)
			end;
		_ ->
			Bin
	end;
readpacket(Bin) ->
	Bin.


constr_header(Len, ID, RespTo, OP) ->
	<<(Len+16):32/little, ID:32/little, RespTo:32/little, OP:32/little>>.

constr_update(U, Name) ->
	Update = <<0:32, Name/binary, 0:8, (U#update.upsert):32/little>>,
	Sz = byte_size(Update) + byte_size(U#update.selector) + byte_size(U#update.document),
	Header = constr_header(Sz, 0, 0, ?OP_UPDATE),
	[Header, Update, U#update.selector, U#update.document].

constr_insert(U, Name) ->
	Insert = <<0:32, Name/binary, 0:8>>,
	Header = constr_header(byte_size(Insert)+byte_size(U#insert.documents), 0, 0, ?OP_INSERT),
	[Header, Insert, U#insert.documents].

constr_query(U, Index, Name) ->
	QueryBody = [U#search.criteria, U#search.field_selector],
	Query = <<(U#search.opts):32/little, Name/binary, 0:8, (U#search.nskip):32/little, (U#search.ndocs):32/little>>,
	Header = constr_header(byte_size(Query)+iolist_size(QueryBody), Index, 0, ?OP_QUERY),
	[Header,Query,QueryBody].

constr_getmore(U, Index, Name) ->
	GetMore = <<0:32, Name/binary, 0:8, (U#cursor.limit):32/little, (U#cursor.id):64/little>>,
	Header = constr_header(byte_size(GetMore), Index, 0, ?OP_GET_MORE),
	[Header, GetMore].

constr_delete(U, Name) ->
	Delete = <<0:32, Name/binary, 0:8, 0:32, (U#delete.selector)/binary>>,
	Header = constr_header(byte_size(Delete), 0, 0, ?OP_DELETE),
	[Header, Delete].

constr_killcursors(U) ->
	Kill = <<0:32, (byte_size(U#killc.cur_ids) div 8):32/little, (U#killc.cur_ids)/binary>>,
	Header = constr_header(byte_size(Kill), 0, 0, ?OP_KILL_CURSORS),
	[Header, Kill].

% Taken and modified from
% https://github.com/comtihon/mongodb-erlang/blob/master/src/connection/mc_auth_logic.erl

%% @private
scram_first_step_start(P) ->
  RandomBString = base64:encode(crypto:strong_rand_bytes(32)),
  FirstMessage = compose_first_message(P#auth.us, RandomBString),
  Message = <<"n,,", FirstMessage/binary>>,
	Doc = [{<<"saslStart">>, 1},
		{<<"mechanism">>, <<"SCRAM-SHA-1">>},
		{<<"payload">>, {binary, Message}},
		{<<"autoAuthorize">>, 1}],
	self() ! {find, self(), <<"admin.$cmd">>,
		#search{nskip = 0, ndocs = 1, criteria = bson:encode(Doc)}},
	P#auth{nonce = RandomBString, first_msg = FirstMessage, step = 1}.

scram_step(#auth{step = 1} = P, Res1) ->
  % {true, Res} = mc_worker_api:sync_command(Socket, <<"admin">>,
  %   {<<"saslStart">>, 1, <<"mechanism">>, <<"SCRAM-SHA-1">>, <<"payload">>, {bin, bin, Message}, <<"autoAuthorize">>, 1}, SetOpts),
	[Res] = bson:decode(map, Res1),
	ConversationId = maps:get(<<"conversationId">>, Res, {}),
  Payload = maps:get(<<"payload">>, Res),
  scram_second_step_start(P, Payload, ConversationId);
scram_step(#auth{step = 2} = P, Res1) ->
	[Res] = bson:decode(map, Res1),
	scram_third_step_start(P, base64:encode(P#auth.sig), Res);
scram_step(#auth{step = 4} = P, Res1) ->
	[#{<<"done">> := true}] = bson:decode(map, Res1),
	init_auth(P#auth.source, undefined, undefined).


%% @private
scram_second_step_start(P, {binary, _, Decoded} = _Payload, ConversationId) ->
  {Signature, ClientFinalMessage} = compose_second_message(Decoded, P#auth.us, P#auth.pw, P#auth.nonce, P#auth.first_msg),
  % {true, Res} = mc_worker_api:sync_command(Socket, <<"admin">>, {<<"saslContinue">>, 1, <<"conversationId">>, ConversationId,
  %   <<"payload">>, {bin, bin, ClientFinalMessage}}, SetOpts),
	Doc = [{<<"saslContinue">>, 1},
	{<<"conversationId">>, ConversationId},
  {<<"payload">>, {binary,  ClientFinalMessage}}],
	self() ! {find, self(), <<"admin.$cmd">>,
		#search{nskip = 0, ndocs = 1, criteria = bson:encode(Doc)}},
	P#auth{sig = Signature, step = 2, conv_id = ConversationId}.

%% @private
scram_third_step_start(P, ServerSignature, Response) ->
  {binary, _, Payload} = maps:get(<<"payload">>, Response),
  Done = maps:get(<<"done">>, Response, false),
  ParamList = parse_server_responce(Payload),
  {_,ServerSignature} = lists:keyfind(<<"v">>,1, ParamList),
  scram_forth_step_start(P, Done).

%% @private
scram_forth_step_start(P, true) -> init_auth(P, undefined, undefined);
scram_forth_step_start(P, false) ->
	Doc = [{<<"saslContinue">>, 1},
	{<<"conversationId">>, P#auth.conv_id},
	{<<"payload">>, {binary, <<>>}}],
	self() ! {find, self(), <<"admin.$cmd">>,
			#search{nskip = 0, ndocs = 1, criteria = bson:encode(Doc)}},
	P#auth{step = 4}.

%% @private
compose_first_message(Login, RandomBString) ->
  UserName = <<<<"n=">>/binary, (encode_name(Login))/binary>>,
  Nonce = <<<<"r=">>/binary, RandomBString/binary>>,
  <<UserName/binary, <<",">>/binary, Nonce/binary>>.

encode_name(Name) ->
  Comma = re:replace(Name, <<"=">>, <<"=3D">>, [{return, binary}]),
	re:replace(Comma, <<",">>, <<"=2C">>, [{return, binary}]).

%% @private
compose_second_message(Payload, Login, Password, RandomBString, FirstMessage) ->
  ParamList = parse_server_responce(Payload),
  {_,R} = lists:keyfind(<<"r">>,1, ParamList),
  Nonce = <<"r=", R/binary>>,
	RandSz = byte_size(RandomBString),
	<<RandomBString:RandSz/binary,_/binary>> = R,
  {_,S} = lists:keyfind(<<"s">>,1, ParamList),
  I = binary_to_integer(element(2,lists:keyfind(<<"i">>,1, ParamList))),
  SaltedPassword = pbkdf2(pw_hash(Login, Password), base64:decode(S), I, 20),
  ChannelBinding = <<"c=", (base64:encode(<<"n,,">>))/binary>>,
  ClientFinalMessageWithoutProof = <<ChannelBinding/binary, ",", Nonce/binary>>,
  AuthMessage = <<FirstMessage/binary, ",", Payload/binary, ",", ClientFinalMessageWithoutProof/binary>>,
  ServerSignature = generate_sig(SaltedPassword, AuthMessage),
  Proof = generate_proof(SaltedPassword, AuthMessage),
  {ServerSignature, <<ClientFinalMessageWithoutProof/binary, ",", Proof/binary>>}.

pw_hash(Username, Password) ->
	bson:dec2hex(<<>>, crypto:hash(md5, [Username, <<":mongo:">>, Password])).

%% @private
generate_proof(SaltedPassword, AuthMessage) ->
  ClientKey = crypto:hmac(sha, SaltedPassword, <<"Client Key">>),
  StoredKey = crypto:hash(sha, ClientKey),
  Signature = crypto:hmac(sha, StoredKey, AuthMessage),
  ClientProof = xorKeys(ClientKey, Signature, <<>>),
  <<"p=", (base64:encode(ClientProof))/binary>>.

%% @private
generate_sig(SaltedPassword, AuthMessage) ->
  ServerKey = crypto:hmac(sha, SaltedPassword, "Server Key"),
  crypto:hmac(sha, ServerKey, AuthMessage).

%% @private
xorKeys(<<>>, _, Res) -> Res;
xorKeys(<<FA, RestA/binary>>, <<FB, RestB/binary>>, Res) ->
  xorKeys(RestA, RestB, <<Res/binary, <<(FA bxor FB)>>/binary>>).

%% @private
parse_server_responce(Responce) ->
  ParamList = binary:split(Responce, <<",">>, [global]),
  lists:map(
    fun(Param) ->
      [K, V] = binary:split(Param, <<"=">>),
      {K, V}
end, ParamList).

pbkdf2(Password, Salt, Iterations, DerivedLength) ->
	pbkdf2(Password, Salt, Iterations, DerivedLength, 1, []).
pbkdf2(Password, Salt, Iterations, DerivedLength, BlockIndex, Acc) ->
	case iolist_size(Acc) > DerivedLength of
		true ->
			<<Bin:DerivedLength/binary, _/binary>> = iolist_to_binary(lists:reverse(Acc)),
			Bin;
		false ->
			Block = pbkdf2(Password, Salt, Iterations, BlockIndex, 1, <<>>, <<>>),
			pbkdf2(Password, Salt, Iterations, DerivedLength, BlockIndex + 1, [Block | Acc])
end.
pbkdf2(_Password, _Salt, Iterations, _BlockIndex, Iteration, _Prev, Acc) when Iteration > Iterations ->
	Acc;
pbkdf2(Password, Salt, Iterations, BlockIndex, 1, _Prev, _Acc) ->
	InitialBlock = crypto:hmac(sha,Password, <<Salt/binary, BlockIndex:32/integer>>),
	pbkdf2(Password, Salt, Iterations, BlockIndex, 2, InitialBlock, InitialBlock);
pbkdf2(Password, Salt, Iterations, BlockIndex, Iteration, Prev, Acc) ->
	Next = crypto:hmac(sha,Password, Prev),
	pbkdf2(Password, Salt, Iterations, BlockIndex, Iteration + 1, Next, crypto:exor(Next, Acc)).
