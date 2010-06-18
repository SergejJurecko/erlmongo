-module(mongodb).
-export([deser_prop/1,reload/0, print_info/0, start/0, stop/0, init/1, handle_call/3, 
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
% API
-export([connect/1, connect/2, is_connected/1,deleteConnection/1, singleServer/2, singleServer/1, masterSlave/3,masterMaster/3, replicaPairs/3, 
		 datetime_to_now/1]).
% Internal
-export([exec_cursor/3, exec_delete/3, exec_cmd/3, exec_insert/3, exec_find/3, exec_update/3, exec_getmore/3,  
         encoderec/1, encode_findrec/1, encoderec_selector/2, gen_keyname/2, gen_prop_keyname/2, rec/0, recoffset/1, recfields/1,
         decoderec/2, encode/1, decode/1, ensureIndex/3, clearIndexCache/0, create_id/0, startgfs/1, dec2hex/2, hex2dec/2]).
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

rec() ->
	receive
		X ->
			io:format("~p~n", [X])
		after 1000 ->
			done
	end.

reload() ->
	gen_server:call(?MODULE, {reload_module}).
	% code:purge(?MODULE),
	% code:load_file(?MODULE),
	% spawn(fun() -> register() end).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).
	
% register() ->
% 	supervisor:start_child(supervisor, {?MODULE, {?MODULE, start, []}, permanent, 1000, worker, [?MODULE]}).
		
print_info() ->
	gen_server:cast(?MODULE, {print_info}).

% % SPEED TEST
% loop(N, B) ->
% 	Start = now(),
% 	io:format("~p~n", [Start]),
% 	t(N, B),
% 	Stop = now(),
% 	io:format("~p, ~p~n", [Stop, timer:now_diff(Stop,Start)]).
% 
% t(0, _) ->
% 	true;
% t(N, R) ->
% 	% encoderec(#mydoc{name = <<"IZ_RECORDA">>, address = #address{city = <<"ny">>, country = <<"USA">>}, i = 12, tags = {array, [<<"abc">>, <<"def">>]}}),
% 	decoderec(#mydoc{}, R),
% 	% ensureIndex(#mydoc{}, [{#mydoc.name, -1},{#mydoc.i, 1}]),
% 	% decode(R),
% 	t(N-1, R).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%								API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect(Pool) ->
	gen_server:cast(?MODULE, {start_connection, Pool, undefined}).
% For when connection is established. Parameter can be:
% - {Module,Function,Params}
% - PID, that gets a {mongodb_connected} message
connect(Pool, Callback) when is_pid(Callback); is_tuple(Callback), tuple_size(Callback) == 3 ->
	gen_server:cast(?MODULE, {start_connection, Pool, Callback}).

deleteConnection(Pool) ->
	gen_server:cast(?MODULE,{delete_connection,Pool}).

is_connected(Pool) ->
	gen_server:call(?MODULE, {is_connected,Pool}).
	
singleServer(Pool) ->
	% gen_server:cast(?MODULE, {conninfo,Pool, {replicaPairs, {"localhost",?MONGO_PORT}, {"localhost",?MONGO_PORT}}}).
	gen_server:cast(?MODULE, {conninfo,Pool, {masterSlave, {"localhost",?MONGO_PORT}, {"localhost",?MONGO_PORT}}}).
singleServer(Pool,Addr) ->
	[IP,Port] = string:tokens(Addr,":"),
	% gen_server:cast(?MODULE, {conninfo, {single, {Addr,Port}}}).
	gen_server:cast(?MODULE, {conninfo,Pool, {masterSlave, {IP,Port}, {IP,Port}}}).
masterSlave(Pool,MasterAddr, SlaveAddr) ->
	[IP1,Port1] = string:tokens(MasterAddr,":"),
	[IP2,Port2] = string:tokens(SlaveAddr,":"),
	gen_server:cast(?MODULE, {conninfo,Pool, {masterSlave, {IP1,Port1}, {IP2,Port2}}}).
masterMaster(Pool,Addr1,Addr2) ->
	[IP1,Port1] = string:tokens(Addr1,":"),
	[IP2,Port2] = string:tokens(Addr2,":"),
	gen_server:cast(?MODULE, {conninfo,Pool, {masterMaster, {IP1,Port1}, {IP2,Port2}}}).
replicaPairs(Pool,Addr1,Addr2) ->
	[IP1,Port1] = string:tokens(Addr1,":"),
	[IP2,Port2] = string:tokens(Addr2,":"),
	gen_server:cast(?MODULE, {conninfo,Pool, {replicaPairs, {IP1,Port1}, {IP2,Port2}}}).
	
datetime_to_now(Loctime) ->	
	Secs = calendar:datetime_to_gregorian_seconds(Loctime) - 719528 * 24 * 60 * 60,
	{Secs div 1000000, Secs rem 1000000,0}.
	
ensureIndex(Pool,DB,Bin) ->
	gen_server:cast(?MODULE, {ensure_index,Pool, DB, Bin}).
clearIndexCache() ->
	gen_server:cast(?MODULE, {clear_indexcache}).

exec_cursor(Pool,Col, Quer) ->
	case trysend(Pool,{find, self(), Col, Quer},1) of
		ok ->
			receive
				{query_result, _Src, <<_:32,CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
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
			case trysend(Pool,{getmore, self(), Col, C},1) of
				ok ->
					receive
						{query_result, _Src, <<_:32,CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
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
							{done, <<>>}
					end;
				X ->
					X
			end
	end.
exec_delete(Pool,Collection, D) ->
	% case gen_server:call(?MODULE, {getwrite,Pool}) of
	% 	undefined ->
	% 		not_connected;
	% 	PID ->
	% 		PID ! {delete, Collection, D},
	% 		ok
	% end.
	trysend(Pool,{delete,Collection,D},1).
exec_find(Pool,Collection, Quer) ->
	% case gen_server:call(?MODULE, {getread,Pool}) of
	case trysend(Pool,{find, self(), Collection, Quer},1) of
		ok ->
			receive
				{query_result, _Src, <<_:32,_CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
					Result
				after 20000 ->
					not_connected
			end;
		X ->
			X
	end.
exec_insert(Pool,Collection, D) ->
	% case gen_server:call(?MODULE, {getwrite,Pool}) of
	% 	undefined ->
	% 		not_connected;
	% 	PID ->
	% 		PID ! {insert, Collection, D},
	% 		ok
	% end.
	trysend(Pool,{insert,Collection,D},1).
exec_update(Pool,Collection, D) ->
	% case gen_server:call(?MODULE, {getwrite,Pool}) of
	% 	undefined ->
	% 		not_connected;
	% 	PID ->
	% 		PID ! {update, Collection, D},
	% 		ok
	% end.
	trysend(Pool,{update,Collection,D},1).
exec_cmd(Pool,DB, Cmd) ->
	Quer = #search{ndocs = 1, nskip = 0, criteria = mongodb:encode(Cmd)},
	case exec_find(Pool,<<DB/binary, ".$cmd">>, Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			[];
		Result ->
			case mongodb:decode(Result) of
				[Res] ->
					Res;
				Res ->
					Res
			end
	end.

trysend(_,_,N) when N > 2 ->
	not_connected;
trysend(Pool,Query,N) ->
	case catch Pool ! Query of
		{'EXIT',_} ->
			timer:sleep(1000),
			trysend(Pool,Query,N+1);
		_ ->
			ok
	end.
		
create_id() ->
	dec2hex(<<>>, gen_server:call(?MODULE, {create_oid})).

startgfs(P) ->
	PID = spawn_link(fun() -> gfs_proc(P,<<>>) end),
	PID ! {start},
	PID.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%								IMPLEMENTATION
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Process dictionary: 
% {PoolName, #conn{}}
% {ConnectionPID, PoolName}

% read = connection used for reading (find) from mongo server
% write = connection used for writing (insert,update) to mongo server
%   single: same as replicaPairs (single server is always master and used for read and write)
%   masterSlave: read = write = master
%   replicaPairs: read = write = master
%   masterMaster: pick one at random
% timer is reconnect timer if some connection is missing
-record(conn, {pid, timer, conninfo, cb}).
% indexes is ensureIndex cache (an ets table).
-record(mngd, {indexes, hashed_hostn, oid_index = 1}).
-define(R2P(Record), rec2prop(Record, record_info(fields, mngd))).
-define(P2R(Prop), prop2rec(Prop, mngd, #mngd{}, record_info(fields, mngd))).	
	

handle_call({create_oid}, _, P) ->
	WC = element(1,erlang:statistics(wall_clock)) rem 16#ffffffff,
	% <<_:20/binary,PID:2/binary,_/binary>> = term_to_binary(self()),
	N = P#mngd.oid_index rem 16#ffffff,
	{reply, <<WC:32, (P#mngd.hashed_hostn)/binary, (list_to_integer(os:getpid())):16, N:24>>, P#mngd{oid_index = P#mngd.oid_index + 1}};
handle_call({is_connected,Name}, _, P) ->
	case get(Name) of
		X when is_pid(X#conn.pid) ->
			{reply, true, P};
		_X ->
			{reply, false, P}
	end;
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call({reload_module}, _, P) ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	{reply, ok, ?MODULE:deser_prop(?R2P(P))};
handle_call(_, _, P) ->
	{reply, ok, P}.

deser_prop(P) ->
	?P2R(P).

startcon(Name, undefined, Type, Addr, Port) when is_list(Port) ->
	startcon(Name, undefined, Type, Addr, list_to_integer(Port));
startcon(Name, undefined, Type, Addr, Port) ->
	PID = spawn_link(fun() -> connection(true) end),
	put(PID,Name),
	% register(Name,PID),
	PID ! {start, Name, self(), Type, Addr, Port};
startcon(_,PID, _, _, _) ->
	PID.
	
handle_cast({ensure_index,Pool, DB, Bin}, P) ->
	case ets:lookup(P#mngd.indexes, {DB,Bin}) of
		[] ->
			spawn(fun() -> exec_insert(Pool,<<DB/binary, ".system.indexes">>, #insert{documents = Bin}) end),
			ets:insert(P#mngd.indexes, {{DB,Bin}});
		_ ->
			true
	end,
	{noreply, P};
handle_cast({clear_indexcache}, P) ->
	ets:delete_all_objects(P#mngd.indexes),
	{noreply, P};
handle_cast({conninfo, Pool, Info}, P) ->
	case get(Pool) of
		undefined ->
			put(Pool,#conn{conninfo = Info});
		#conn{pid = undefined} = _PI ->
			true;
		PI ->
			case PI#conn.conninfo of
				Info ->
					true;
				_ ->
					put(Pool,#conn{conninfo = Info}),
					PI#conn.pid ! {stop}
			end
	end,
	self() ! {save_connections},
	{noreply, P};
handle_cast({start_connection, Pool}, P) ->
	handle_cast({start_connection,Pool,undefined}, P);
handle_cast({start_connection, Pool, CB}, P) ->
	case get(Pool) of
		undefined ->
			true;
		PI ->
			start_connection(Pool, PI#conn{cb = CB})
	end,
	{noreply, P};
handle_cast({delete_connection, Pool}, P) ->
	case get(Pool) of
		undefined ->
			true;
		PI ->
			case is_pid(PI#conn.pid) of
				true ->
					PI#conn.pid ! {stop};
				_ ->
					true
			end
	end,
	erase(Pool),
	self() ! {save_connections},
	{noreply, P};
handle_cast({print_info}, P) ->
	io:format("~p ~p~n~p~n", [self(),get(),?R2P(P)]),
	{noreply, P};
handle_cast(_, P) ->
	{noreply, P}.

start_connection(Name, #conn{conninfo = {masterMaster, {A1,P1},{A2,P2}}} = P) ->
	case P#conn.pid of
		undefined ->
			Timer = P#conn.timer,
			case random:uniform(2) of
				1 ->
					startcon(Name,P#conn.pid,readwrite,A1,P1);
				2 ->
					startcon(Name,P#conn.pid,readwrite,A2,P2)
			end;
		_ ->
			Timer = ctimer(P#conn.timer)
	end,
	put(Name,P#conn{timer = Timer});
start_connection(Name, #conn{conninfo = {masterSlave, {A1,P1},{_A2,_P2}}} = P)  ->
	case P#conn.pid of
		undefined ->
			Timer = P#conn.timer,
			startcon(Name,P#conn.pid,readwrite,A1,P1);
		_ ->
			Timer = ctimer(P#conn.timer)
	end,
	put(Name,P#conn{timer = Timer});
start_connection(Name, #conn{conninfo = {replicaPairs, {A1,P1},{A2,P2}}} = P)  ->
	case P#conn.pid of
		undefined ->
			% io:format("starting~n"),
			Timer = P#conn.timer,
			startcon(Name, undefined, ifmaster, A1,P1),
			startcon(Name, undefined, ifmaster, A2,P2);
		_ ->
			Timer = ctimer(P#conn.timer)
	end,
	put(Name,P#conn{timer = Timer});
start_connection(_,_) ->
	true.

ctimer(undefined) ->
	undefined;
ctimer(T) ->
	timer:cancel(T),
	undefined.

timer(undefined,Pool) ->
	{ok, Timer} = timer:send_interval(?RECONNECT_DELAY, {reconnect,Pool}),
	Timer;
timer(T,_) ->
	T.

conn_callback(P) ->
	case is_pid(P) of
		true ->
			P ! {mongodb_connected};
		false ->
			case P of
				{Mod,Fun,Param} ->
					erlang:apply(Mod,Fun,Param);
				_ ->
					true
			end
	end.

handle_info({conn_established, Pool, readwrite, ConnProc}, P) ->
	case get(Pool) of
		undefined ->
			true;
		PI ->
			put(ConnProc,Pool),
			put(Pool,PI#conn{pid = ConnProc}),
			conn_callback(PI#conn.cb)
	end,
	{noreply, P};
handle_info({reconnect, Pool}, P) ->
	handle_cast({start_connection, Pool}, P);
handle_info({'EXIT', PID,W}, P) ->
	% io:format("condied ~p~n", [{PID,_W}]),
	case get(PID) of
		undefined ->
			true;
		Pool ->
			erase(PID),
			conndied(Pool,PID,get(Pool)),
			case W of
				% Most likely died because of code reload, restart immediately
				killed ->
					self() ! {reconnect,Pool};
				_ ->
					true
			end
	end,
	{noreply, P};
handle_info({save_connections}, P) ->
	L = lists:foldl(fun({Pool,#conn{} = I},L) ->
					[{Pool,I#conn.conninfo}|L];
				   (_,L) ->
					L
				   end,[],get()),
	application:set_env(erlmongo,connections,L),
	{noreply, P};
handle_info({query_result, Src, <<_:20/binary, Res/binary>>}, P) ->
	PI = get(get(Src)),
	% io:format("~p~n", ["RES"]),
	try mongodb:decode(Res) of
		[[{<<"ismaster">>, 1}|_]] when element(1,PI#conn.conninfo) == replicaPairs, PI#conn.pid == undefined ->
			link(Src),
			% io:format("~p, registering ~p~n", ["Foundmaster", registered()]),
			conn_callback(PI#conn.cb),
			put(get(Src),PI#conn{pid = Src}),
			register(get(Src),Src),
			{noreply, P};
		_X ->
			% io:format("~p~n", [_X]),
			Src ! {stop},
			{noreply, P}
	catch
		error:_X ->
			% io:format("~p~n", [_X]),
			Src ! {stop},
			{noreply, P}
	end;
handle_info({query_result, Src, _}, P) ->
	Src ! {stop},
	{noreply, P};
handle_info(_X, P) -> 
	io:format("~p~n", [_X]),
	{noreply, P}.
	

conndied(Name,_PID,P) ->
	put(Name, P#conn{pid = undefined, timer = timer(P#conn.timer, Name)}).

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	% timer:send_interval(1000, {timeout}),
	case application:get_env(erlmongo,connections) of
		{ok, L} ->
			[gen_server:cast(?MODULE,{conninfo, Pool, Info}) || {Pool,Info} <- L],
			[connect(Pool) || {Pool,_} <- L];
		_ ->
			true
	end,
	{ok, HN} = inet:gethostname(),
	<<HashedHN:3/binary,_/binary>> = erlang:md5(HN),
	process_flag(trap_exit, true),
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
			exec_update(P#gfs_state.pool,<<(P#gfs_state.collection)/binary, ".files">>, #update{selector = mongodb:encode([{<<"_id">>, {oid, FileID}}]), 
																		document = mongodb:encoderec(P#gfs_state.file)}),
			Keys = [{<<"files_id">>, 1},{<<"n">>,1}],
			Bin = mongodb:encode([{plaintext, <<"name">>, gen_prop_keyname(Keys, <<>>)},
			 					  {plaintext, <<"ns">>, <<(P#gfs_state.collection)/binary, ".chunks">>},
			                      {<<"key">>, {bson, encode(Keys)}}]),
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
			% io:format("reading ~p, ~p~n", [N, byte_size(Buf)]),
			case true of
				_ when N =< byte_size(Buf) ->
					% io:format("cached ~p ~p ~n", [N, byte_size(Buf)]),
					<<Ret:N/binary, Rem/binary>> = Buf,
					Source ! {gfs_bytes, Ret},
					gfs_proc(P, Rem);
				_ ->
					GetChunks = ((N - byte_size(Buf)) div CSize) + 1,
					% io:format("Finding buf ~p, getchunks ~p, skip ~p~n", [byte_size(Buf), GetChunks,P#gfs_state.nchunk]),
					Quer = #search{ndocs = GetChunks, nskip = 0, 
								   criteria = mongodb:encode([{<<"files_id">>, (P#gfs_state.file)#gfs_file.docid},
															  {<<"n">>, {in,{gte, P#gfs_state.nchunk},{lte, P#gfs_state.nchunk + GetChunks}}}]), 
								   field_selector = get(field_selector)},
					% io:format("find ~p~n", [{P#gfs_state.pool,P#gfs_state.collection}]),
					case mongodb:exec_find(P#gfs_state.pool,<<(P#gfs_state.collection)/binary, ".chunks">>, Quer) of
						not_connected ->
							Source ! not_connected,
							gfs_proc(P,Buf);
						<<>> ->
							% io:format("Noresult~n"),
							Source ! eof,
							gfs_proc(P,Buf);
						ResBin ->
							% io:format("Result ~p~n", [ResBin]),
							Result = chunk2bin(mongodb:decode(ResBin), <<>>),
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
			% put(criteria, mongodb:encode([{<<"files_id">>, {oid, (P#gfs_state.file)#gfs_file.docid}}])),
			put(field_selector, mongodb:encode([{<<"data">>, 1}])),
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
					 Rem, <<Out/binary, (mongodb:encoderec(Chunk))/binary>>);
		Rem when P#gfs_state.closed == true, byte_size(Rem) > 0 ->
			Chunk = #gfs_chunk{docid = {oid,create_id()}, files_id = FileID, n = P#gfs_state.nchunk, data = {binary, 2, Rem}},
			gfsflush(P#gfs_state{length = P#gfs_state.length + byte_size(Rem)}, 
			         <<>>, <<Out/binary, (mongodb:encoderec(Chunk))/binary>>);
		Rem when byte_size(Out) > 0 ->
			File = P#gfs_state.file,
			exec_insert(P#gfs_state.pool,<<(P#gfs_state.collection)/binary, ".chunks">>, #insert{documents = Out}),
			case P#gfs_state.closed of
				true ->
					[{<<"md5">>, MD5}|_] = exec_cmd(P#gfs_state.pool,P#gfs_state.db, [{<<"filemd5">>, FileID},{<<"root">>, P#gfs_state.collection}]);
				false ->
					MD5 = undefined
			end,
			exec_update(P#gfs_state.pool,<<(P#gfs_state.collection)/binary, ".files">>, #update{selector = mongodb:encode([{<<"_id">>, FileID}]), 
																		document = mongodb:encoderec(File#gfs_file{length = P#gfs_state.length,
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


-record(con, {sock}).
% Proc. d.:
%   {ReqID, ReplyPID}
% Waiting for request
connection(_) ->
	connection(#con{},1,<<>>).
connection(#con{} = P,Index,Buf) ->
	receive
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
		{stop} ->
			true;
		{start, Pool, Source, Type, IP, Port} ->
			{A1,A2,A3} = now(),
		    random:seed(A1, A2, A3),
			{ok, Sock} = gen_tcp:connect(IP, Port, [binary, {packet, 0}, {active, true}, {keepalive, true}]),
			case Type of
				ifmaster ->
					self() ! {find, Source, <<"admin.$cmd">>, #search{nskip = 0, ndocs = 1, criteria = mongodb:encode([{<<"ismaster">>, 1}])}};
				_ ->
					register(Pool,self()),
					Source ! {conn_established, Pool, Type, self()}
			end,
			connection(#con{sock = Sock},1, <<>>);
		{tcp_closed, _} ->
			exit(stop)
	end.
readpacket(Bin) ->
	<<ComplSize:32/little, _ReqID:32/little,RespID:32/little,_OpCode:32/little, Body/binary>> = Bin,
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
	end.

	
constr_header(Len, ID, RespTo, OP) ->
	<<(Len+16):32/little, ID:32/little, RespTo:32/little, OP:32/little>>.

constr_update(U, Name) ->
	Update = <<0:32, Name/binary, 0:8, 
	           (U#update.upsert):32/little, (U#update.selector)/binary, (U#update.document)/binary>>,
	Header = constr_header(byte_size(Update), 0, 0, ?OP_UPDATE),
	<<Header/binary, Update/binary>>.

constr_insert(U, Name) ->
	Insert = <<0:32, Name/binary, 0:8, (U#insert.documents)/binary>>,
	Header = constr_header(byte_size(Insert), 0, 0, ?OP_INSERT),
	<<Header/binary, Insert/binary>>.

constr_query(U, Index, Name) ->
	Query = <<(U#search.opts):32/little, Name/binary, 0:8, (U#search.nskip):32/little, (U#search.ndocs):32/little, 
	  		  (U#search.criteria)/binary, (U#search.field_selector)/binary>>,
	Header = constr_header(byte_size(Query), Index, 0, ?OP_QUERY),
	<<Header/binary,Query/binary>>.

constr_getmore(U, Index, Name) ->
	GetMore = <<0:32, Name/binary, 0:8, (U#cursor.limit):32/little, (U#cursor.id):64/little>>,
	Header = constr_header(byte_size(GetMore), Index, 0, ?OP_GET_MORE),
	<<Header/binary, GetMore/binary>>.

constr_delete(U, Name) ->
	Delete = <<0:32, Name/binary, 0:8, 0:32, (U#delete.selector)/binary>>,
	Header = constr_header(byte_size(Delete), 0, 0, ?OP_DELETE),
	<<Header/binary, Delete/binary>>.
	
constr_killcursors(U) ->
	Kill = <<0:32, (byte_size(U#killc.cur_ids) div 8):32/little, (U#killc.cur_ids)/binary>>,
	Header = constr_header(byte_size(Kill), 0, 0, ?OP_KILL_CURSORS),
	<<Header/binary, Kill/binary>>.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%						BSON encoding/decoding 
%	basic BSON encoding/decoding taken and modified from the mongo-erlang-driver project by Elias Torres
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

recfields(Rec) ->
	case true of
		_ when is_tuple(Rec) ->
			RecFields = get({recinfo, element(1,Rec)});
		_ when is_atom(Rec) ->
			RecFields = get({recinfo, Rec})
	end,
	case RecFields of
		undefined ->
			[_|Fields] = element(element(2, Rec), ?RECTABLE),
			Fields;
		[recindex|Fields] ->
			Fields;
		Fields ->
			Fields
	end.
recoffset(Rec) ->
	case true of
		_ when is_tuple(Rec) ->
			RecFields = get({recinfo, element(1,Rec)});
		_ when is_atom(Rec) ->
			RecFields = get({recinfo, Rec})
	end,
	case RecFields of
		undefined ->
			3;
		[recindex|_] ->
			3;
		_ ->
			2
	end.

encoderec(Rec) ->
	% [_|Fields] = element(element(2, Rec), ?RECTABLE),
	Fields = recfields(Rec),
	% io:format("~p~n", [Fields]),
	encoderec(<<>>, deep, Rec, Fields, recoffset(Rec), <<>>).
encode_findrec(Rec) ->
	% [_|Fields] = element(element(2, Rec), ?RECTABLE),
	Fields = recfields(Rec),
	encoderec(<<>>, flat, Rec, Fields, recoffset(Rec), <<>>).
	
encoderec(NameRec, Type, Rec, [{FieldName, _RecIndex}|T], N, Bin) ->
	case element(N, Rec) of
		undefined ->
			encoderec(NameRec, Type, Rec, T, N+1, Bin);
		SubRec when Type == flat ->
			% [_|SubFields] = element(element(2, SubRec), ?RECTABLE),
			SubFields = recfields(SubRec),
			case NameRec of
				<<>> ->
					Dom = atom_to_binary(FieldName, latin1);
				_ ->
					Dom = <<NameRec/binary, ".", (atom_to_binary(FieldName, latin1))/binary>>
			end,
			encoderec(NameRec, Type, Rec, T, N+1, <<Bin/binary, (encoderec(Dom, flat, SubRec, SubFields, 3, <<>>))/binary>>);
		Val ->
			encoderec(NameRec, Type, Rec, T, N+1, <<Bin/binary, (encode_element({atom_to_binary(FieldName, latin1), {bson, encoderec(Val)}}))/binary>>)
	end;
encoderec(NameRec, Type, Rec, [FieldName|T], N, Bin) ->
	case element(N, Rec) of
		undefined ->
			encoderec(NameRec, Type,Rec, T, N+1, Bin);
		Val ->
			case FieldName of
				docid ->
					case Val of
						{oid, _} ->
							encoderec(NameRec, Type,Rec, T, N+1, <<Bin/binary, (encode_element({<<"_id">>, {oid, Val}}))/binary>>);
						_ ->
							encoderec(NameRec, Type,Rec, T, N+1, <<Bin/binary, (encode_element({<<"_id">>, Val}))/binary>>)
					end;
				_ ->
					case NameRec of
						<<>> ->
							Dom = atom_to_binary(FieldName, latin1);
						_ ->
							Dom = <<NameRec/binary, ".", (atom_to_binary(FieldName, latin1))/binary>>
					end,
					encoderec(NameRec, Type,Rec, T, N+1, <<Bin/binary, (encode_element({Dom, Val}))/binary>>)
			end
	end;
encoderec(<<>>,_,_, [], _, Bin) ->
	<<(byte_size(Bin)+5):32/little, Bin/binary, 0:8>>;
encoderec(_,_,_, [], _, Bin) ->
	% <<(byte_size(Bin)+5):32/little, Bin/binary, 0:8>>.
	Bin.

encoderec_selector(_, undefined) ->
	<<>>;
encoderec_selector(_, <<>>) ->
	<<>>;
encoderec_selector(Rec, SelectorList) ->
	% [_|Fields] = element(element(2, Rec), ?RECTABLE),
	Fields = recfields(Rec),
	encoderec_selector(SelectorList, Fields, recoffset(Rec), <<>>).

% SelectorList is either a list of indexes in the record tuple, or a list of {TupleIndex, TupleVal}. Use the index to get the name
% from the list of names.
encoderec_selector([{FieldIndex, Val}|Fields], [FieldName|FieldNames], FieldIndex, Bin) ->
	case FieldName of
		docid ->
			encoderec_selector(Fields, FieldNames, FieldIndex+1, <<Bin/binary, (encode_element({<<"_id">>, Val}))/binary>>);
		{Name, _RecIndex} ->
			encoderec_selector(Fields, FieldNames, FieldIndex+1, <<Bin/binary, (encode_element({atom_to_binary(Name,latin1), Val}))/binary>>);
		_ ->
			encoderec_selector(Fields, FieldNames, FieldIndex+1, <<Bin/binary, (encode_element({atom_to_binary(FieldName,latin1), Val}))/binary>>)
	end;
encoderec_selector([FieldIndex|Fields], [FieldName|FieldNames], FieldIndex, Bin) ->
	case FieldName of
		docid ->
			encoderec_selector(Fields, FieldNames, FieldIndex+1, <<Bin/binary, (encode_element({<<"_id">>, 1}))/binary>>);
		{Name, _RecIndex} ->
			encoderec_selector(Fields, FieldNames, FieldIndex+1, <<Bin/binary, (encode_element({atom_to_binary(Name,latin1), 1}))/binary>>);
		_ ->
			encoderec_selector(Fields, FieldNames, FieldIndex+1, <<Bin/binary, (encode_element({atom_to_binary(FieldName,latin1), 1}))/binary>>)
	end;
encoderec_selector(Indexes, [_|Names], Index, Bin) ->
	encoderec_selector(Indexes, Names, Index+1, Bin);
encoderec_selector([], _, _, Bin) ->
	<<(byte_size(Bin)+5):32/little, Bin/binary, 0:8>>.

gen_prop_keyname([{[_|_] = KeyName, KeyVal}|T], Bin) ->
	gen_prop_keyname([{list_to_binary(KeyName), KeyVal}|T], Bin);
gen_prop_keyname([{KeyName, KeyVal}|T], Bin) ->
	case is_integer(KeyVal) of
		true ->
			Add = <<(list_to_binary(integer_to_list(KeyVal)))/binary>>;
		false ->
			Add = <<>>
	end,
	gen_prop_keyname(T, <<Bin/binary, KeyName/binary, "_", Add/binary>>);
gen_prop_keyname([], B) ->
	B.
	
gen_keyname(Rec, Keys) ->
	% [_|Fields] = element(element(2, Rec), ?RECTABLE),
	Fields = recfields(Rec),
	gen_keyname(Keys, Fields, recoffset(Rec), <<>>).

gen_keyname([{KeyIndex, KeyVal}|Keys], [Field|Fields], KeyIndex, Name) ->
	case Field of
		{FieldName, _} ->
			true;
		FieldName ->
			true
	end,
	case is_integer(KeyVal) of
		true ->
			Add = <<(list_to_binary(integer_to_list(KeyVal)))/binary>>;
		false ->
			Add = <<>>
	end,
	gen_keyname(Keys, Fields, KeyIndex+1, <<Name/binary, "_", (atom_to_binary(FieldName, latin1))/binary, "_", Add/binary>>);
gen_keyname([], _, _, <<"_", Name/binary>>) ->
	Name;
gen_keyname(Keys, [_|Fields], KeyIndex, Name) ->
	% [{I,_}|_] = Keys,
	gen_keyname(Keys, Fields, KeyIndex+1, Name).
	

decoderec(Rec, <<>>) ->
	% Rec;
	erlang:make_tuple(tuple_size(Rec), undefined, [{1, element(1,Rec)}, {2, element(2,Rec)}]);
decoderec(Rec, Bin) ->
	% [_|Fields] = element(element(2, Rec), ?RECTABLE),
	Fields = recfields(Rec),
	case recoffset(Rec) of
		3 ->
			decode_records([], Bin, tuple_size(Rec), element(1,Rec), element(2, Rec), Fields);
		_ ->
			decode_records([], Bin, tuple_size(Rec), element(1,Rec), undefined, Fields)
	end.
	

decode_records(RecList, <<_ObjSize:32/little, Bin/binary>>, TupleSize, Name, TabIndex, Fields) ->
	case TabIndex of
		undefined ->
			{FieldList, Remain} = get_fields([], Fields, 2, Bin),
			NewRec = erlang:make_tuple(TupleSize, undefined, [{1, Name}|FieldList]);
		_ ->
			{FieldList, Remain} = get_fields([], Fields, 3, Bin),
			NewRec = erlang:make_tuple(TupleSize, undefined, [{1, Name},{2, TabIndex}|FieldList])
	end,
	decode_records([NewRec|RecList], Remain, TupleSize, Name, TabIndex, Fields);
decode_records(R, <<>>, _, _, _, _) ->
	lists:reverse(R).

get_fields(RecVals, Fields, Offset, Bin) ->
	case rec_field_list(RecVals, Offset, Fields, Bin) of
		{again, SoFar, Rem} ->
			get_fields(SoFar, Fields, Offset, Rem);
		Res ->
			Res
	end.

rec_field_list(RecVals, _, _, <<0:8, Rem/binary>>) ->
	{RecVals, Rem};
	% done;
rec_field_list(RecVals, _, [], <<Type:8, Bin/binary>>) ->
	{_Name, ValRem} = decode_cstring(Bin, <<>>),
	{_Value, Remain} = decode_value(Type, ValRem),
	{again, RecVals, Remain};
rec_field_list(RecVals, N, [Field|Fields], <<Type:8, Bin/binary>>) ->
	% io:format("~p~n", [Field]),
	{Name, ValRem} = decode_cstring(Bin, <<>>),
	case Field of
		docid ->
			BinName = <<"_id">>;
		{Fn, _} ->
			BinName = atom_to_binary(Fn, latin1);
		Fn ->
			BinName = atom_to_binary(Fn, latin1)
	end,
	case BinName of
		Name ->
			case Field of
				{RecName, RecIndex} ->
					<<LRecSize:32/little, RecObj/binary>> = ValRem,
					RecSize = LRecSize - 4,
					<<RecBin:RecSize/binary, Remain/binary>> = RecObj,
					case is_integer(RecIndex) of
						true ->
							[_|RecFields] = element(RecIndex, ?RECTABLE),
							RecLen = length(RecFields)+2;
						false ->
							RecFields = recfields(RecName),
							RecLen = length(RecFields)+recoffset(RecName)-1
					end,
					[Value] = decode_records([], <<LRecSize:32/little, RecBin/binary>>, RecLen, 
													RecName, RecIndex, RecFields),
					rec_field_list([{N, Value}|RecVals], N+1, Fields, Remain);
				_ ->
					{Value, Remain} = decode_value(Type, ValRem),
					rec_field_list([{N, Value}|RecVals], N+1, Fields, Remain)
					% case Value of
					% 	{oid, V} ->
					% 		rec_field_list([{N, V}|RecVals], N+1, Fields, Remain);
					% 	_ ->
					% 		rec_field_list([{N, Value}|RecVals], N+1, Fields, Remain)
					% end
			end;
		_ ->
			rec_field_list(RecVals, N+1, Fields, <<Type:8, Bin/binary>>)
	end.


dec2hex(N, <<I:8,Rem/binary>>) ->
	dec2hex(<<N/binary, (hex0((I band 16#f0) bsr 4)):8, (hex0((I band 16#0f))):8>>, Rem);
dec2hex(N,<<>>) ->
	N.

hex2dec(N,{oid, Bin}) ->
	hex2dec(N, Bin);
hex2dec(N,<<A:8,B:8,Rem/binary>>) ->
	hex2dec(<<N/binary, ((dec0(A) bsl 4) + dec0(B)):8>>, Rem);
hex2dec(N,<<>>) ->
	N.

dec0($a) ->	10;
dec0($b) ->	11;
dec0($c) ->	12;
dec0($d) ->	13;
dec0($e) ->	14;
dec0($f) ->	15;
dec0(X) ->	X - $0.

hex0(10) -> $a;
hex0(11) -> $b;
hex0(12) -> $c;
hex0(13) -> $d;
hex0(14) -> $e;
hex0(15) -> $f;
hex0(I) ->  $0 + I.


encode(undefined) ->
	<<>>;
encode(<<>>) ->
	<<>>;
encode(Items) ->
	Bin = lists:foldl(fun(Item, B) -> <<B/binary, (encode_element(Item))/binary>> end, <<>>, Items),
    <<(byte_size(Bin)+5):32/little-signed, Bin/binary, 0:8>>.

encode_element({[_|_] = Name, Val}) ->
	encode_element({list_to_binary(Name),Val});
encode_element({Name, [{_,_}|_] = Items}) ->
	Binary = encode(Items),
	<<3, Name/binary, 0, Binary/binary>>;
encode_element({Name, []}) ->
	<<2, Name/binary, 0, 1:32/little-signed, 0>>;
encode_element({Name, [_|_] = Value}) ->
	ValueEncoded = encode_cstring(Value),
	<<2, Name/binary, 0, (byte_size(ValueEncoded)):32/little-signed, ValueEncoded/binary>>;
encode_element({Name, <<_/binary>> = Value}) ->
	ValueEncoded = encode_cstring(Value),
	<<2, Name/binary, 0, (byte_size(ValueEncoded)):32/little-signed, ValueEncoded/binary>>;
encode_element({Name, Value}) when is_integer(Value) ->
	case true of
		_ when Value >= 2147483648; Value =< -2147483648 ->
			<<18, Name/binary, 0, Value:64/little-signed>>;
		_ ->
			<<16, Name/binary, 0, Value:32/little-signed>>
	end;
encode_element({plaintext, Name, Val}) -> % exists for performance reasons.
	<<2, Name/binary, 0, (byte_size(Val)+1):32/little-signed, Val/binary, 0>>;
encode_element({Name, true}) ->
	<<8, Name/binary, 0, 1:8>>;
encode_element({Name, false}) ->
	<<8, Name/binary, 0, 0:8>>;	
encode_element({Name, {oid, OID}}) ->
	<<7, Name/binary, 0, (hex2dec(<<>>, OID))/binary>>;
% list of lists = array
encode_element({Name, {array, Items}}) ->
  	<<4, Name/binary, 0, (encarray([], Items, 0))/binary>>;
encode_element({Name, {bson, Bin}}) ->
	<<3, Name/binary, 0, Bin/binary>>;
encode_element({Name, {binary, 2, Data}}) ->
  	<<5, Name/binary, 0, (byte_size(Data)+4):32/little-signed, 2:8, (byte_size(Data)):32/little-signed, Data/binary>>;
encode_element({Name,{struct,Items}}) ->
	Binary = encode(Items),
	<<3, Name/binary, 0, Binary/binary>>;
encode_element({Name, {inc, Val}}) ->
	encode_element({<<"$inc">>, [{Name, Val}]});
encode_element({Name, {set, Val}}) ->
	encode_element({<<"$set">>, [{Name, Val}]});
encode_element({Name, {unset, Val}}) ->
	encode_element({<<"$unset">>, [{Name, Val}]});
encode_element({Name, {push, Val}}) ->
	encode_element({<<"$push">>, [{Name, Val}]});
encode_element({Name, {pushAll, Val}}) ->
	encode_element({<<"$pushAll">>, [{Name, {array, Val}}]});
encode_element({Name, {pop, Val}}) ->
	encode_element({<<"$pop">>, [{Name, Val}]});
encode_element({Name, {pull, Val}}) ->
	encode_element({<<"$pull">>, [{Name, Val}]});
encode_element({Name, {pullAll, Val}}) ->
	encode_element({<<"$pullAll">>, [{Name, {array, Val}}]});
encode_element({Name, {addToSet, {array,Val}}}) ->
	encode_element({<<"$addToSet">>, [{Name, [{<<"$each">>, {array, Val}}]}]});
encode_element({Name, {addToSet, Val}}) ->
	encode_element({<<"$addToSet">>, [{Name, Val}]});
encode_element({Name, {gt, Val}}) ->
	encode_element({Name, [{<<"$gt">>, Val}]});
encode_element({Name, {lt, Val}}) ->
	encode_element({Name, [{<<"$lt">>, Val}]});
encode_element({Name, {lte, Val}}) ->
	encode_element({Name, [{<<"$lte">>, Val}]});
encode_element({Name, {gte, Val}}) ->
	encode_element({Name, [{<<"$gte">>, Val}]});
encode_element({Name, {ne, Val}}) ->
	encode_element({Name, [{<<"$ne">>, Val}]});
encode_element({Name, {in, {FE,FV},{TE,TV}}}) ->
	encode_element({Name, [{<<"$", (atom_to_binary(FE,latin1))/binary>>, FV},
						   {<<"$", (atom_to_binary(TE,latin1))/binary>>, TV}]});
encode_element({Name, {in, Val}}) ->
	encode_element({Name, [{<<"$in">>, {array, Val}}]});
encode_element({Name, {nin, Val}}) ->
	encode_element({Name, [{<<"$nin">>, {array, Val}}]});
encode_element({Name, {mod, By,Rem}}) ->
	encode_element({Name, [{<<"$mod">>, {array, [By,Rem]}}]});
encode_element({Name, {all, Val}}) ->
	encode_element({Name, [{<<"$all">>, {array, Val}}]});
encode_element({Name, {size, Val}}) ->
	encode_element({Name, [{<<"$size">>, Val}]});
encode_element({Name, {'not', Val}}) ->
	encode_element({Name, [{<<"$not">>, Val}]});
encode_element({Name, {exists, Val}}) ->
	encode_element({Name, [{<<"$exists">>, Val}]});
encode_element({Name, {binary, SubType, Data}}) ->
  	StringEncoded = encode_cstring(Name),
  	<<5, StringEncoded/binary, (byte_size(Data)):32/little-signed, SubType:8, Data/binary>>;
encode_element({Name, Value}) when is_float(Value) ->
	<<1, (Name)/binary, 0, Value:64/little-signed-float>>;
encode_element({Name, {obj, []}}) ->
	<<3, Name/binary, 0, (encode([]))/binary>>;	
encode_element({Name, {MegaSecs, Secs, MicroSecs}}) when  is_integer(MegaSecs),is_integer(Secs),is_integer(MicroSecs) ->
  Unix = MegaSecs * 1000000 + Secs,
  Millis = Unix * 1000 + (MicroSecs div 1000),
  <<9, Name/binary, 0, Millis:64/little-signed>>;
encode_element({Name, null}) ->
  <<10, Name/binary, 0>>;
encode_element({Name, {regex, Expression, Flags}}) ->
  ExpressionEncoded = encode_cstring(Expression),
  FlagsEncoded = encode_cstring(Flags),
  <<11, Name/binary, 0, ExpressionEncoded/binary, FlagsEncoded/binary>>;
encode_element({Name, {ref, Collection, <<First:8/little-binary-unit:8, Second:4/little-binary-unit:8>>}}) ->
  CollectionEncoded = encode_cstring(Collection),
  FirstReversed = lists:reverse(binary_to_list(First)),
  SecondReversed = lists:reverse(binary_to_list(Second)),
  OID = list_to_binary(lists:append(FirstReversed, SecondReversed)),
  <<12, Name/binary, 0, (byte_size(CollectionEncoded)):32/little-signed, CollectionEncoded/binary, OID/binary>>;
encode_element({Name, {code, Code}}) ->
  CodeEncoded = encode_cstring(Code),
  <<13, Name/binary, 0, (byte_size(CodeEncoded)):32/little-signed, CodeEncoded/binary>>;
encode_element({Name,{bignum,Value}}) ->
	<<18, Name/binary, 0, Value:64/little-signed>>;
% code with scope
encode_element({Name, {code, C, S}}) ->
	Code = encode_cstring(C),
	Scope = encode(S),
	<<15, Name/binary, 0, (8+byte_size(Code)+byte_size(Scope)):32/little, (byte_size(Code)):32/little, Code/binary, Scope/binary>>.
	
	
encarray(L, [H|T], N) ->
	encarray([{integer_to_list(N), H}|L], T, N+1);
encarray(L, [], _) ->
	encode(lists:reverse(L)).

encode_cstring(String) ->
    <<(unicode:characters_to_binary(String))/binary, 0:8>>.
	
%% Size has to be greater than 4
% decode(<<Size:32/little-signed, Rest/binary>> = Binary) when byte_size(Binary) >= Size, Size > 4 ->
% 	decode(Rest, Size-4);
% 
% decode(_BadLength) ->
% 	throw({invalid_length}).
% 
% decode(Binary, _Size) ->
%   	case decode_next(Binary, []) of
%     	{BSON, <<>>} ->
%       		[BSON];
%     	{BSON, Rest} ->
% 			[BSON | decode(Rest)]
%   	end.
decode(Bin) ->
	% io:format("Decoding ~p~n", [Bin]),
	decode(Bin,[]).
decode(<<_Size:32, Bin/binary>>, L) ->
  	{BSON, Rem} = decode_next(Bin, []),
	decode(Rem,[BSON|L]);
decode(<<>>, L) ->
	lists:reverse(L).

decode_next(<<>>, Accum) ->
  	{lists:reverse(Accum), <<>>};
decode_next(<<0:8, Rest/binary>>, Accum) ->
	{lists:reverse(Accum), Rest};
decode_next(<<Type:8/little, Rest/binary>>, Accum) ->
  	{Name, EncodedValue} = decode_cstring(Rest, <<>>),
% io:format("Decoding ~p~n", [Type]),
  	{Value, Next} = decode_value(Type, EncodedValue),
  	decode_next(Next, [{Name, Value}|Accum]).

decode_cstring(<<>>, _) ->
	throw({invalid_cstring});
decode_cstring(<<0:8, Rest/binary>>, Acc) ->
	{Acc, Rest};
decode_cstring(<<C:8, Rest/binary>>, Acc) ->
	decode_cstring(Rest, <<Acc/binary, C:8>>).

decode_value(7, <<OID:12/binary,Rest/binary>>) ->
  	{{oid, dec2hex(<<>>, OID)}, Rest};
decode_value(16, <<Integer:32/little-signed, Rest/binary>>) ->
	{Integer, Rest};
decode_value(18, <<Integer:64/little-signed, Rest/binary>>) ->
	{Integer, Rest};
decode_value(1, <<Double:64/little-signed-float, Rest/binary>>) ->
	{Double, Rest};
decode_value(2, <<Size:32/little-signed, Rest/binary>>) ->
	StringSize = Size-1,
	case Rest of
		<<String:StringSize/binary, 0:8, Remain/binary>> ->
			{String, Remain};
		<<String:Size/binary, Remain/binary>> ->
			{String,Remain}
	end;
decode_value(3, <<Size:32/little-signed, Rest/binary>> = Binary) when byte_size(Binary) >= Size ->
  	decode_next(Rest, []);
decode_value(4, <<Size:32/little-signed, Data/binary>> = Binary) when byte_size(Binary) >= Size ->
  	{Array, Rest} = decode_next(Data, []),
  	{{array,[Value || {_Key, Value} <- Array]}, Rest};
decode_value(5, <<_Size:32/little-signed, 2:8/little, BinSize:32/little-signed, BinData:BinSize/binary-little-unit:8, Rest/binary>>) ->
  	{{binary, 2, BinData}, Rest};
decode_value(5, <<Size:32/little-signed, SubType:8/little, BinData:Size/binary-little-unit:8, Rest/binary>>) ->
  	{{binary, SubType, BinData}, Rest};
decode_value(6, _Binary) ->
  	throw(encountered_undefined);
decode_value(8, <<0:8, Rest/binary>>) ->
	{false, Rest};
decode_value(8, <<1:8, Rest/binary>>) ->
  	{true, Rest};
decode_value(9, <<Millis:64/little-signed, Rest/binary>>) ->
	UnixTime = Millis div 1000,
  	MegaSecs = UnixTime div 1000000,
  	Secs = UnixTime - (MegaSecs * 1000000),
  	MicroSecs = (Millis - (UnixTime * 1000)) * 1000,
  	{{MegaSecs, Secs, MicroSecs}, Rest};
decode_value(10, Binary) ->
  	{null, Binary};
decode_value(11, Binary) ->
  	{Expression, RestWithFlags} = decode_cstring(Binary, <<>>),
  	{Flags, Rest} = decode_cstring(RestWithFlags, <<>>),
  	{{regex, Expression, Flags}, Rest};
decode_value(12, <<Size:32/little-signed, Data/binary>> = Binary) when size(Binary) >= Size ->
	{NS, RestWithOID} = decode_cstring(Data, <<>>),
	{{oid, OID}, Rest} = decode_value(7, RestWithOID),
	{{ref, NS, OID}, Rest};
decode_value(13, <<_Size:32/little-signed, Data/binary>>) ->
	{Code, Rest} = decode_cstring(Data, <<>>),
	{{code, Code}, Rest};
decode_value(14, _Binary) ->
	% throw(encountered_ommitted);
	decode_value(2,_Binary);
decode_value(15, <<ComplSize:32/little, StrBSize:32/little,Rem/binary>>) ->
	StrSize = StrBSize - 1,
	ScopeSize = ComplSize - 8 - StrBSize,
	<<Code:StrSize/binary, _, Scope:ScopeSize/binary,Rest/binary>> = Rem,
	{{code,Code,decode(Scope)}, Rest};
decode_value(18, <<Integer:32/little-signed, Rest/binary>>) ->
	{Integer, Rest}.
	
	
	
	