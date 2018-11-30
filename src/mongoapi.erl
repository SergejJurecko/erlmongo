-module(mongoapi).
-compile(nowarn_export_all).
-compile(export_all).
-include_lib("erlmongo.hrl").

new(Pool,DB) ->
	{?MODULE,[Pool,DB]}.

set_encode_style(mochijson,{?MODULE,[Pool,DB]}) ->
	put({Pool, DB, style}, mochijson);
set_encode_style(default,{?MODULE,[Pool,DB]}) ->
	put({Pool, DB, style}, default).

set_encode_style({?MODULE,[Pool,DB]}) ->
	put({Pool, DB, style}, default).


name([_|_] = Collection,PMI) ->
	name(list_to_binary(Collection), PMI);
name(<<_/binary>> = Collection,{?MODULE,[_Pool,DB]}) ->
	<<DB/binary, ".", Collection/binary>>;
name(Collection,PMI) when is_atom(Collection) ->
	name(atom_to_binary(Collection, latin1),PMI).

remove(Col, Selector, PMI) ->
	% mongodb:exec_delete(Pool,name(Col,{?MODULE,[Pool,DB]}), #delete{selector = bson:encode(Selector)}).
	case runCmd([{delete, Col}, {deletes, {array,[[{q, Selector},{limit,0}]]}}],PMI) of
		[_|_] = Obj ->
			case [lists:keyfind(Find,1,Obj) || Find <- [<<"ok">>] ] of
				[{_,OK}] when OK > 0 ->
					ok;
				Invalid ->
					{error,Invalid}
			end;
		E ->
			E
	end.


save(Collection, L, {?MODULE,[Pool,DB]}) ->
	% Style = case get({Pool, DB, style}) of
	% 	undefined -> default;
	% 	T -> T
  %       end,
	case getid(L) of
		false ->
			OID = mongodb:create_id(),
			case L of
				#{} ->
					L1 = L#{<<"_id">> => {oid, OID}};
				_ ->
					L1 = [{<<"_id">>, {oid, OID}}|L]
			end,
			case insert(Collection, L1, {?MODULE,[Pool,DB]}) of
				ok ->
					{ok,{oid, OID}};
				R ->
					R
			end;
		{_, OID} ->
			Sel = [{<<"_id">>, OID}],
			case update(Collection,Sel, L, [upsert], {?MODULE,[Pool,DB]}) of
				ok ->
					{ok,OID};
				R ->
					R
			end
	end.

getid(L) when is_list(L) ->
	lists:keyfind(<<"_id">>,1,L);
getid(#{} = L) ->
	case maps:get(<<"_id">>,L,false) of
		false ->
			false;
		Id ->
			{<<"_id">>,Id}
	end.


% Examples:
%  update([{#mydoc.name, "docname"}], #mydoc{name = "different name"}, [upsert])
%  update([{#mydoc.name, "docname"}], #mydoc{i = {inc, 1}}, [upsert])
%  update([{#mydoc.name, "docname"}], #mydoc{tags = {push, "lamer"}}, [])
%  update([{#mydoc.name, "docname"}], #mydoc{tags = {pushAll, ["dumbass","jackass"]}}, [upsert])
%  update([{#mydoc.name, "docname"}], #mydoc{tags = {pullAll, ["dumbass","jackass"]}}, [upsert])
%  update([{#mydoc.name,"ime"}],#mydoc{i = {addToSet,{array,[1,2,3,4]}}},[upsert])
%  update([{#mydoc.name,"ime"}],#mydoc{i = {addToSet,10}}},[upsert]).
%  and so on.
% modifier list: inc, set, push, pushAll, pop, pull, pullAll
% Flags can be: [upsert,multi]
update(Col, Selector, Doc, Flags,PMI) ->
	run_update(Col, [Selector], [Doc], Flags,PMI).
batchUpdate(Col, Selectors, Docs, Flags, PMI) ->
	run_update(Col, Selectors, Docs, Flags,PMI).

run_update(Col, Sels, Docs, Flags,PMI) ->
	[Upsert,Multi] = [lists:member(S,Flags) || S <- [upsert,multi]],
	L = [[{q, Sel}, {u, Doc}, {upsert, Upsert}, {multi, Multi}] || {Sel, Doc} <- lists:zip(Sels,Docs)],
	case runCmd([{update, Col}, {updates, {array,L}}],PMI) of
		[_|_] = Obj ->
			% io:format("Update ~p~n",[Obj]),
			case [lists:keyfind(Find,1,Obj) || Find <- [<<"ok">>,<<"n">>] ] of
				[{_,OK},{_,N}] when OK > 0, N > 0 ->
					ok;
				Invalid ->
					{error,Invalid}
			end;
		E ->
			% io:format("update ERROR ~p~n",[E]),
			E
	end.

encbu(L, [Sel|ST],[[_|_] = Doc|DT],Flags) ->
	encbu([#update{selector = bson:encode(Sel), document = bson:encode(Doc), upsert = Flags}|L],ST,DT,Flags);
encbu(L,[],[],_) ->
	L.

updateflags([upsert|T],V) ->
	updateflags(T,V bor 1);
updateflags([multi|T],V) ->
	updateflags(T,V bor 2);
updateflags([], V) ->
	V.

insert(Col, L, PMI) ->
	run_insert(Col,[L], PMI).

run_insert(Col,L,PMI) ->
	case runCmd([{insert, Col}, {documents, {array,L}}],PMI) of
		[_|_] = Obj ->
			case [lists:keyfind(Find,1,Obj) || Find <- [<<"ok">>,<<"n">>] ] of
				[{_,OK},{_,N}] when OK > 0, N > 0 ->
					ok;
				Invalid ->
					{error,Invalid}
			end;
		E ->
			% io:format("Insert ERROR ~p~n",[E]),
			E
	end.

batchInsert(Col, [_|_] = LRecs, PMI) ->
	run_insert(Col, LRecs, PMI).


% Advanced queries:
%  Regex:                            Mong:find(#mydoc{name = {regex, "(.+?)\.flv", "i"}}, undefined,0,0)
%  Documents with even i:            Mong:find(#mydoc{i = {mod, 2, 0}}, undefined, 0,0).
%  Documents with i larger than 2:   Mong:find(#mydoc{i = {gt, 2}}, undefined, 0,0).
%  Documents with i between 2 and 5: Mong:find(#mydoc{i = {in, {gt, 2}, {lt, 5}}}, undefined, 0,0).
%  in example:     Mong:find(#mydoc{tags = {in, [2,3,4]}}, undefined, 0,0).
%  exists example: Mong:find(#mydoc{tags = {exists, false}}, undefined, 0,0).
%  Advanced query operators: gt,lt,gte,lte, ne, in, nin, all, size, exists,'not'
%  Possible regex options: "ilmsux" -> IN THIS SEQUENCE! (not all are necessary of course)
% 	i 	 case-insensitive matching
%	m 	multiline: "^" and "$" match the beginning / end of each line as well as the whole string
%	x 	verbose / comments: the pattern can contain comments
%	l (lowercase L) 	locale: \w, \W, etc. depend on the current locale
%	s 	dotall: the "." character matches everything, including newlines
%	u 	unicode: \w, \W, etc. match unicode
findOne(Col, Q, {?MODULE,[Pool,DB]}) ->
	findOne(Col, Q, proplist, {?MODULE,[Pool,DB]}).
findOne(Col, [], Format, {?MODULE,[Pool,DB]}) ->
	case find(Col, [], undefined, 0, 1,Format,{?MODULE,[Pool,DB]}) of
		{ok, [Res]} -> {ok, Res};
		{ok, []} -> {ok, []};
		R ->
			R
	end;
findOne(Col, Query, Format,{?MODULE,[Pool,DB]}) when Format == proplist; Format == map ->
	case find(Col, Query, undefined, 0, 1, Format,{?MODULE,[Pool,DB]}) of
		{ok, [Res]} -> {ok, Res};
		{ok, []} -> {ok, []};
		R ->
			R
	end;
findOne(Col, Query, Selector,{?MODULE,[Pool,DB]}) ->
	findOne(Col, Query, Selector,proplist,{?MODULE,[Pool,DB]}).
findOne(Col, Query, Selector,Format,{?MODULE,[Pool,DB]}) ->
	case find(Col, Query, Selector, 0, 1, Format,{?MODULE,[Pool,DB]}) of
		{ok, [Res]} -> {ok, Res};
		{ok, []} -> {ok, []};
		R ->
			R
	end.

find(Col, #search{} = Q,{?MODULE,[Pool,DB]}) ->
	find(Col, Q#search.criteria, Q#search.field_selector, Q#search.nskip, Q#search.ndocs,{?MODULE,[Pool,DB]}).

% Format = [proplist | map]
find(Col, Query, Selector, From, Limit,{?MODULE,[Pool,DB]}) ->
	find(Col, Query, Selector, From, Limit, proplist,{?MODULE,[Pool,DB]}).
find(Col, Query, Selector, From, Limit,Format,{?MODULE,[Pool,DB]}) ->
	Quer = #search{ndocs = Limit, nskip = From, criteria = bson:encode(Query), field_selector = bson:encode(Selector)},
	case mongodb:exec_find(Pool,name(Col,{?MODULE,[Pool,DB]}), Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			{ok, []};
		Res ->
			{ok, bson:decode(Format,Res)}
	end.

% opts: [[map | proplist],reverse, {sort, SortyBy}, explain, {hint, Hint}, snapshot]
% SortBy: {key, Val} or a list of keyval tuples -> {i,1}  (1 = ascending, -1 = descending)
% Hint: [{Key,Val}] -> [{#mydoc.i,1}]
findOpt(Col, Query, Selector, Opts, From, Limit,{?MODULE,[Pool,DB]}) ->
	case Query of
		[] ->
			{_,Format,Q} = translateopts(false,undefined, Opts,[{<<"query">>, {bson,bson:encode([])}}],proplist);
		_ ->
			{_,Format,Q} = translateopts(false,undefined, Opts,[{<<"query">>, Query}],proplist)
	end,
	find(Col, Q, Selector, From, Limit,Format,{?MODULE,[Pool,DB]}).


findOpt(Col, #search{} = Q, Opts,{?MODULE,[Pool,DB]}) ->
	findOpt(Col, Q#search.criteria, Q#search.field_selector, Opts, Q#search.nskip, Q#search.ndocs,{?MODULE,[Pool,DB]}).

cursor(Col,Query, Selector, Opts, From, Limit,{?MODULE,[Pool,DB]}) ->
	cursor(Col,Query, Selector, Opts, From, Limit, proplist,{?MODULE,[Pool,DB]}).
cursor(Col,Query, Selector, Opts, From, Limit,Format,{?MODULE,[Pool,DB]}) ->
	{_,Format,Q} = translateopts(false,Query, Opts,[{<<"query">>, {bson, bson:encode(Query)}}], Format),
	Quer = #search{ndocs = Limit, nskip = From, field_selector = bson:encode(Selector),
		criteria = bson:encode(Q),opts = ?QUER_OPT_CURSOR},
	case mongodb:exec_cursor(Pool,name(Col,{?MODULE,[Pool,DB]}), Quer) of
		not_connected ->
			not_connected;
		{done, <<>>} ->
			{done, []};
		{done, Result} ->
			{done, bson:decode(Format,Result)};
		{Cursor, Result} ->
			{ok, Cursor, bson:decode(Format,Result)}
	end.

getMore(Rec, Cursor,{?MODULE,[Pool,_DB]}) when is_list(Rec); is_binary(Rec) ->
	case mongodb:exec_getmore(Pool,Rec, Cursor) of
		not_connected ->
			not_connected;
		{done, <<>>} ->
			{done, []};
		{done, Result} ->
			{done, bson:decode(Result)};
		{ok, Result} ->
			{ok, bson:decode(Result)}
	end.
closeCursor(Cur,_PMI) ->
	Cur#cursor.pid ! {cleanup},
	ok.

translateopts(H,Q, [proplist|T], L, _F) ->
	translateopts(H,Q, T, L, proplist);
translateopts(H,Q, [map|T], L, _F) ->
	translateopts(H,Q, T, L, map);
translateopts(H,undefined, [{sort, SortBy}|T], L,F) when is_list(SortBy); is_map(SortBy) ->
	translateopts(H,undefined, T, [{<<"orderby">>, SortBy}|L],F);
translateopts(H,undefined, [{sort, {Key,Val}}|T], L,F) ->
	translateopts(H,undefined, T, [{<<"orderby">>, [{Key,Val}]}|L],F);
translateopts(H,Rec, [reverse|T], L,F) ->
	translateopts(H,Rec, T, [{<<"orderby">>, [{<<"$natural">>, -1}]}|L],F);
translateopts(_,Rec, [explain|T], L,F) ->
	translateopts(true,Rec, T, [{<<"$explain">>, true}|L],F);
translateopts(H,Rec, [snapshot|T], L,F) ->
	translateopts(H,Rec, T, [{<<"$snapshot">>, true}|L],F);
translateopts(H,undefined, [{hint, Hint}|T], L,F) ->
	translateopts(H,undefined, T, [{<<"$hint">>, [{Hint, 1}]}|L],F);
translateopts(H,_, [], L,F) ->
	{H,F,L}.

% If you wish to index on an embedded document, use proplists.
% Example: ensureIndex(<<"mydoc">>, [{<<"name">>, 1}]).
% 		   ensureIndex(<<"mydoc">>,[{<<"name",1}],[{"unique",true}]).
%  You can use lists, they will be turned into binaries.
ensureIndex(Collection, Keys,{?MODULE,[Pool,DB]}) ->
	ensureIndex(Collection,Keys,[],{?MODULE,[Pool,DB]}).
ensureIndex([_|_] = Collection, Keys,Opts,{?MODULE,[Pool,DB]}) ->
	ensureIndex(list_to_binary(Collection), Keys,Opts,{?MODULE,[Pool,DB]});
ensureIndex(<<_/binary>> = Collection, Keys,Opts,{?MODULE,[Pool,DB]}) ->
	Obj = [{plaintext, <<"name">>, bson:gen_prop_keyname(Keys, <<>>)},
	 			 {plaintext, <<"ns">>, name(Collection,{?MODULE,[Pool,DB]})},
	       {<<"key">>, {bson, bson:encode(Keys)}}|Opts],
	Bin = bson:encode(Obj),
	mongodb:ensureIndex(Pool,DB, Bin).
% Example: ensureIndex(#mydoc{}, [{#mydoc.name, 1}])

deleteIndexes([_|_] = Collection,{?MODULE,[Pool,DB]}) ->
	deleteIndexes(list_to_binary(Collection),{?MODULE,[Pool,DB]});
deleteIndexes(<<_/binary>> = Collection,{?MODULE,[Pool,DB]}) ->
	mongodb:clearIndexCache(),
	mongodb:exec_cmd(Pool,DB, [{plaintext, <<"deleteIndexes">>, Collection}, {plaintext, <<"index">>, <<"*">>}]).

deleteIndex(Rec, Key,{?MODULE,[Pool,DB]}) ->
	mongodb:clearIndexCache(),
	mongodb:exec_cmd(Pool,DB,[{plaintext, <<"deleteIndexes">>, atom_to_binary(element(1,Rec), latin1)},
				  		 {plaintext, <<"index">>, bson:gen_keyname(Rec,Key)}]).

% How many documents in mydoc collection: Mong:count("mydoc").
%										  Mong:count(#mydoc{}).
% How many documents with i larger than 2: Mong:count(#mydoc{i = {gt, 2}}).
count(Col,{?MODULE,[Pool,DB]}) ->
	count(Col,undefined,{?MODULE,[Pool,DB]}).
count(ColIn, Query,{?MODULE,[Pool,DB]}) ->
	case true of
		_ when is_list(ColIn) ->
			Col = list_to_binary(ColIn);
		_ when is_tuple(ColIn) ->
			Col = atom_to_binary(element(1,ColIn), latin1);
		_ ->
			Col = ColIn
	end,
	case true of
		_ when is_list(Query) ->
			Cmd = [{plaintext, <<"count">>, Col}, {plaintext, <<"ns">>, DB}, {<<"query">>, {bson, bson:encode(Query)}}];
		_ when is_tuple(ColIn), Query == undefined ->
			Cmd = [{plaintext, <<"count">>, Col}, {plaintext, <<"ns">>, DB}, {<<"query">>, {bson, bson:encoderec(ColIn)}}];
		_ when is_tuple(ColIn), is_tuple(Query) ->
			Cmd = [{plaintext, <<"count">>, Col}, {plaintext, <<"ns">>, DB}, {<<"query">>, {bson, bson:encoderec(Query)}}];
		_ when Query == undefined ->
			Cmd = [{plaintext, <<"count">>, Col}, {plaintext, <<"ns">>, DB}]
	end,
	case mongodb:exec_cmd(Pool,DB, Cmd) of
		[_|_] = Obj ->
			case proplists:get_value(<<"n">>,Obj) of
				undefined ->
					false;
				Val ->
					round(Val)
			end;
		_ ->
			false
	end.


addUser(U, P,PMI) when is_binary(U) ->
	addUser(binary_to_list(U),P,PMI);
addUser(U, P,PMI) when is_binary(P) ->
	addUser(U,binary_to_list(P),PMI);
addUser(Username, Password,PMI) ->
	save(<<"system.users">>, [{<<"user">>, Username},
							  {<<"pwd">>, bson:dec2hex(<<>>, erlang:md5(Username ++ ":mongo:" ++ Password))}],PMI).

% Collection: name of collection
% Key (list of fields): [{"i", 1}]
% Reduce: {code, "JS code", Parameters} -> Parameters can be []
% Initial: default values for output object [{"result",0}]
% Optional: [{"$keyf", {code, "JScode",Param}}, {"cond", CondObj}, {"finalize", {code,_,_}}]
% Example: Mong:group("file",[{"ch",1}], {code, "function(doc,out){out.size += doc.size}", []}, [{"size", 0}],[]).
group(Collection, Key,Reduce,Initial,Optional,PMI) ->
	runCmd([{"group", [{<<"ns">>, Collection},
					   {<<"key">>,Key},
					   {<<"$reduce">>, Reduce},
					   {<<"initial">>, Initial}|Optional]}],PMI).

% Mong:eval("function(){return 3+3;}").
% Mong:eval({code, "function(){return what+3;}",[{"what",5}]}).
eval(Code,PMI) ->
	runCmd([{<<"$eval">>, Code}],PMI).


% Runs $cmd. Parameters can be just a string it will be converted into {string,1}
runCmd({_,_} = T,PMI) ->
	runCmd([T],PMI);
runCmd([{_,_}|_] = L,{?MODULE,[Pool,DB]}) ->
	mongodb:exec_cmd(Pool,DB, L);
runCmd([H|_] = L,{?MODULE,[Pool,DB]}) when is_map(H) ->
	mongodb:exec_cmd(Pool,DB, L);
runCmd([_|_] = L,PMI) ->
	runCmd([{L,1}],PMI);
runCmd(<<_/binary>> = L,PMI) ->
	runCmd(binary_to_list(L),PMI).

stats(C,PMI) when is_tuple(C) ->
	stats(atom_to_binary(element(1,C),latin1),PMI);
stats(Collection,PMI) ->
	runCmd([{"collstats", Collection}],PMI).

repairDatabase(PMI) ->
	runCmd([{"repairDatabase", 1}],PMI).
dropDatabase(PMI) ->
	runCmd([{"dropDatabase", 1}],PMI).
cloneDatabase(From,PMI) when is_list(From); is_binary(From) ->
	runCmd([{"clone", From}],PMI).

dropCollection(C,P) when is_tuple(C) ->
	dropCollection(atom_to_binary(element(1,C),latin1),P);
dropCollection(Collection,PMI) ->
	mongodb:clearIndexCache(),
	runCmd([{"drop", Collection}],PMI).

createCollection(Name,{?MODULE,[Pool,DB]}) ->
	createCollection(Name, [],{?MODULE,[Pool,DB]}).
% Options: idindex, noidindex, capped, {size, MaxSizeBytes}, {max, MaxElements}
createCollection(Name, L,{?MODULE,[Pool,DB]}) when is_tuple(Name) ->
	createCollection(atom_to_binary(element(1,Name), latin1), L,{?MODULE,[Pool,DB]});
createCollection(Name, L,{?MODULE,[Pool,DB]}) ->
	runCmd([{<<"create">>, Name}] ++ translatecolopts(L, []),{?MODULE,[Pool,DB]}).

translatecolopts([idindex|T], O) ->
	translatecolopts(T, [{<<"autoIndexId">>, true}|O]);
translatecolopts([noidindex|T], O) ->
	translatecolopts(T, [{<<"autoIndexId">>, false}|O]);
translatecolopts([capped|T], O) ->
	translatecolopts(T, [{<<"capped">>, true}|O]);
translatecolopts([{size, MaxSize}|T], O) ->
	translatecolopts(T, [{<<"size">>, MaxSize}|O]);
translatecolopts([{max, Max}|T], O) ->
	translatecolopts(T, [{<<"max">>, Max}|O]);
translatecolopts([], O) ->
	O.

setProfilingLevel(L,{?MODULE,[Pool,DB]}) when is_integer(L) ->
	case true of
		_ when L > 0 ->
			createCollection(<<"system.profile">>, [capped, {size, 131072}]);
		_ when L >= 0, L =< 2 ->
			true
	end,
	runCmd([{"profile", L}],{?MODULE,[Pool,DB]}).
getProfilingLevel(PMI) ->
	runCmd([{"profile", -1}],PMI).

%
%  Run this before writing any files, or writing will fail!
%
gfsIndexes(PMI) ->
	gfsIndexes(<<"fd">>,PMI).
gfsIndexes(Collection,PMI) ->
	ensureIndex(<<Collection/binary,".chunks">>,[{<<"files_id">>,1},{<<"n">>,1}],PMI),
	ensureIndex(<<Collection/binary,".files">>,[{<<"filename">>,1}],PMI).

gfsNew(Filename,PMI) ->
	gfsNew(<<"fd">>, Filename, [],PMI).
gfsNew(Filename, Opts,PMI) ->
	gfsNew(<<"fd">>, Filename, Opts,PMI).
gfsNew([_|_] = Collection, Filename, Opts,PMI) ->
	gfsNew(list_to_binary(Collection), Filename, Opts,PMI);
gfsNew(<<_/binary>> = Collection, Filename, Opts,{?MODULE,[Pool,DB]}) ->
	mongodb:startgfs(gfsopts(Opts,#gfs_state{pool = Pool,file = #gfs_file{filename = Filename, length = 0, chunkSize = 262144,
		docid = {oid,mongodb:create_id()}, uploadDate = os:timestamp()},
		collection = name(Collection,{?MODULE,[Pool,DB]}),
	 	coll_name = Collection, db = DB, mode = write})).

gfsopts([{meta, Rec}|T], S) ->
	gfsopts(T, S#gfs_state{file = (S#gfs_state.file)#gfs_file{metadata = Rec}});
gfsopts([{aliases, L}|T], S) ->
	gfsopts(T, S#gfs_state{file = (S#gfs_state.file)#gfs_file{aliases = {array, L}}});
gfsopts([{mime, Mime}|T], S) ->
	gfsopts(T, S#gfs_state{file = (S#gfs_state.file)#gfs_file{contentType = Mime}});
gfsopts([{chunkSize, Size}|T], S) ->
	gfsopts(T, S#gfs_state{file = (S#gfs_state.file)#gfs_file{chunkSize = Size}});
gfsopts([{flushLimit, Limit}|T], S) ->
	gfsopts(T, S#gfs_state{flush_limit = Limit});
gfsopts([_|T], S) ->
	gfsopts(T,S);
gfsopts([], S) ->
	S.

gfsWrite(PID, Bin,_P) ->
	PID ! {write, Bin},
	ok.
gfsFlush(PID,_P) ->
	PID ! {flush},
	ok.
gfsClose(PID,_P) ->
	unlink(PID),
	PID ! {close},
	ok.

gfsOpen(R,PMI) ->
	gfsOpen(<<"fd">>, R,PMI).
gfsOpen([_|_] = Col, R,PMI) ->
	gfsOpen(list_to_binary(Col),R,PMI);
gfsOpen(Collection, R,{?MODULE,[Pool,DB]}) ->
	case true of
		_ when R#gfs_file.docid == undefined; R#gfs_file.length == undefined; R#gfs_file.md5 == undefined ->
			Quer = #search{ndocs = 1, nskip = 0, criteria = bson:encode_findrec(R)},
			case mongodb:exec_find(Pool,name(<<Collection/binary, ".files">>,{?MODULE,[Pool,DB]}), Quer) of
				not_connected ->
					not_connected;
				<<>> ->
					[];
				Result ->
					[DR] = bson:decoderec(R, Result),
					gfsOpen(Collection,DR)
			end;
		_ ->
			mongodb:startgfs(#gfs_state{pool = Pool,file = R, coll_name = Collection, collection = name(Collection,{?MODULE,[Pool,DB]}), db = DB, mode = read})
	end.

gfsRead(PID, N, _PMI)	->
	PID ! {read, self(), N},
	receive
		{gfs_bytes, Bin} ->
			Bin
		after 5000 ->
			false
	end.

gfsDelete(R,PMI) ->
	gfsDelete(<<"fd">>, R, PMI).
gfsDelete([_|_] = Col, R, PMI) ->
	gfsDelete(list_to_binary(Col),R, PMI);
gfsDelete(Collection, R, {?MODULE,[Pool,DB]}) ->
	case R#gfs_file.docid of
		undefined ->
			Quer = #search{ndocs = 1, nskip = 0, criteria = bson:encode_findrec(R)},
			case mongodb:exec_find(Pool,name(<<Collection/binary, ".files">>,{?MODULE,[Pool,DB]}), Quer) of
				not_connected ->
					not_connected;
				<<>> ->
					[];
				Result ->
					[DR] = bson:decoderec(R, Result),
					gfsDelete(DR,{?MODULE,[Pool,DB]})
			end;
		_ ->
			% NChunks = (R#gfs_file.length div R#gfs_file.chunkSize) + 1,
			remove(<<Collection/binary, ".files">>, [{<<"_id">>, {oid, R#gfs_file.docid}}],{?MODULE,[Pool,DB]}),
			remove(<<Collection/binary, ".chunks">>, [{<<"files_id">>, {oid, R#gfs_file.docid}}],{?MODULE,[Pool,DB]})
	end.


testw(Mong, Filename) ->
	spawn(fun() ->
			% If the calling process does gfsNew, gfsWrite and dies immediately after,
			%  gfsClose is necesssary. This is because of a race condition.
			%  Both calls will complete before gfs gets the chance to set trap_exit to true and detect
			%  the caller has died.
			{ok,Bin} = file:read_file(Filename),
			Mong:gfsIndexes(),
			PID = Mong:gfsNew(Filename),
			Mong:gfsWrite(PID,Bin),
			Mong:gfsClose(PID)
		  end).
