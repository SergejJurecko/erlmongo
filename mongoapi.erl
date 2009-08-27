-module(mongoapi, [DB]).
% -export([save/1,findOne/2,findOne/1,find/1,find/2,find/3,find/4, update/2, insert/1]).
-compile(export_all).
-include_lib("erlmongo.hrl").

name([_|_] = Collection) ->
	name(list_to_binary(Collection));
name(<<_/binary>> = Collection) ->
	<<DB/binary, ".", Collection/binary>>;
name(Collection) when is_atom(Collection) ->
	name(atom_to_binary(Collection, latin1)).

remove(Rec, Selector) when is_tuple(Rec) ->
	mongodb:exec_delete(name(element(1,Rec)), #delete{selector = mongodb:encoderec_selector(Rec, Selector)});
remove(Col, Selector) ->
	mongodb:exec_delete(name(Col), #delete{selector = mongodb:encode(Selector)}).
	
save(Collection, [_|_] = L) ->
	% io:format("Save on ~p~n", [L]),
	case lists:keysearch(<<"_id">>, 1, L) of
		false ->
			mongodb:exec_insert(name(Collection), #insert{documents = mongodb:encode(L)});
		{value, {_, ID}} ->
			mongodb:exec_update(name(Collection), #update{selector = mongodb:encode([{"_id", ID}]), document = mongodb:encode(L)})
	end.
save(Rec) -> 
	case element(3, Rec) of
		undefined ->
			mongodb:exec_insert(name(element(1,Rec)), #insert{documents = mongodb:encoderec(Rec)});
		ID ->
			mongodb:exec_update(name(element(1,Rec)), #update{selector = mongodb:encode([{"_id", ID}]), document = mongodb:encoderec(Rec)})
	end.


update(Collection, [_|_] = Selector, [_|_] = Doc) ->
	mongodb:exec_update(name(Collection), #update{selector = mongodb:encode(Selector), document = mongodb:encode(Doc)}).
% update([{#mydoc.name, "docname"}], #mydoc{})
update(Selector, Rec) ->
	mongodb:exec_update(name(element(1,Rec)), #update{selector = mongodb:encoderec_selector(Rec, Selector), document = mongodb:encoderec(Rec)}).

insert(Col, [_|_] = L) ->
	mongodb:exec_insert(name(Col), #insert{documents = mongodb:encode(L)}).
insert(Rec) ->
	mongodb:exec_insert(name(element(1,Rec)), #insert{documents = mongodb:encoderec(Rec)}).
	
batchInsert(Col, [[_|_]|_] = LRecs) ->
	DocBin = lists:foldl(fun(L, Bin) -> <<Bin/binary, (mongodb:encode(L))/binary>> end, <<>>, LRecs),
	mongodb:exec_insert(name(Col), #insert{documents = DocBin}).
batchInsert(LRecs) ->
	[FRec|_] = LRecs,
	DocBin = lists:foldl(fun(Rec, Bin) -> <<Bin/binary, (mongodb:encoderec(Rec))/binary>> end, <<>>, LRecs),
	mongodb:exec_insert(name(element(1,FRec)), #insert{documents = DocBin}).

findOne(Col, []) ->
	find(Col, [], undefined, 0, 1);
findOne(Col, [_|_] = Query) when is_tuple(Col) == false ->
	find(Col, Query, undefined, 0, 0);
findOne(Query, Selector) when is_tuple(Query) ->
	[Res] = find(Query, Selector, 0, 1),
	Res.
	
findOne(Query) when is_tuple(Query) ->
	[Res] = find(Query, undefined, 0, 1),
	Res.
findOne(Col, [_|_] = Query, [_|_] = Selector) ->
	find(Col, Query, Selector, 0, 1).

find(Col, #search{} = Q) ->
	find(Col, Q#search.criteria, Q#search.field_selector, Q#search.nskip, Q#search.ndocs).
find(#search{} = Q) ->
	find(Q#search.criteria, Q#search.field_selector, Q#search.nskip, Q#search.ndocs).
	
find(Col, Query, Selector, From, Limit) ->
	Quer = #search{ndocs = Limit, nskip = From, criteria = mongodb:encode(Query), field_selector = mongodb:encode(Selector)},
	case mongodb:exec_find(name(Col), Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			[];
		Res ->
			mongodb:decode(Res)
	end.
find(Query, Selector, From, Limit) ->
	Quer = #search{ndocs = Limit, nskip = From, criteria = mongodb:encoderec(Query), field_selector = mongodb:encoderec_selector(Query, Selector)},
	case mongodb:exec_find(name(element(1,Query)), Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			[];
		Result ->
			mongodb:decoderec(Query, Result)
	end.

% opts: [reverse, {sort, SortyBy}, explain, {hint, Hint}, snapshot]
% SortBy: {key, Val} or a list of keyval tuples -> {i,1}  (1 = ascending, -1 = descending)
% Hint: key
findOpt(Col, Query, Selector, Opts, From, Limit) ->
	find(Col, translateopts(undefined, Opts,[{<<"query">>, Query}]), Selector, From, Limit).
% SortBy examples: {#mydoc.name, 1}, [{#mydoc.i, 1},{#mydoc.name,-1}]
% Hint example: #mydoc.name
findOpt(Query, Selector, Opts, From, Limit) ->
	Quer = #search{ndocs = Limit, nskip = From, field_selector = mongodb:encoderec_selector(Query, Selector),
	             criteria = mongodb:encode(translateopts(Query, Opts,[{<<"query">>, {bson, mongodb:encoderec(Query)}}]))}, 
	case mongodb:exec_find(name(element(1,Query)), Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			[];
		Result ->
			% If opt is explain, it will crash
			try mongodb:decoderec(Query, Result) of
				Res ->
					Res
			catch
				error:_ ->
					mongodb:decode(Result)
			end
	end.
findOpt(Col, #search{} = Q, Opts) ->
	findOpt(Col, Q#search.criteria, Q#search.field_selector, Opts, Q#search.nskip, Q#search.ndocs).
findOpt(#search{} = Q, Opts) ->
	findOpt(Q#search.criteria, Q#search.field_selector, Opts, Q#search.nskip, Q#search.ndocs).
	
cursor(Query, Selector, Opts, From, Limit) ->
	Quer = #search{ndocs = Limit, nskip = From, field_selector = mongodb:encoderec_selector(Query, Selector),
	             criteria = mongodb:encode(translateopts(Query, Opts,[{<<"query">>, {bson, mongodb:encoderec(Query)}}])),
				 opts = ?QUER_OPT_CURSOR},
	case mongodb:exec_cursor(name(element(1,Query)), Quer) of
		not_connected ->
			not_connected;
		{done, <<>>} ->
			{done, []};
		{done, Result} ->
			{done, mongodb:decoderec(Query, Result)};
		{Cursor, Result} ->
			{ok, Cursor, mongodb:decoderec(Query, Result)}
	end.
getMore(Rec, Cursor) ->
	case mongodb:exec_getmore(Cursor) of
		not_connected ->
			not_connected;
		{done, <<>>} ->
			{done, []};
		{done, Result} ->
			{done, mongodb:decoderec(Rec, Result)};
		{ok, Result} ->
			{ok, mongodb:decoderec(Rec, Result)}
	end.
closeCursor(Cur) ->
	Cur#cursor.pid ! {cleanup},
	ok.
	
translateopts(undefined, [{sort, [_|_] = SortBy}|T], L) ->
	translateopts(undefined, T, [{<<"orderby">>, SortBy}|L]);
translateopts(undefined, [{sort, {Key,Val}}|T], L) ->
	translateopts(undefined, T, [{<<"orderby">>, [{Key,Val}]}|L]);
translateopts(Rec, [{sort, [_|_] = SortBy}|T], L) ->
	translateopts(Rec, T, [{<<"orderby">>, {bson, mongodb:encoderec_selector(Rec, SortBy)}}|L]);
translateopts(Rec, [{sort, {Key,Val}}|T], L) ->
	translateopts(Rec, T, [{<<"orderby">>, {bson, mongodb:encoderec_selector(Rec, [{Key,Val}])}}|L]);
translateopts(Rec, [reverse|T], L) ->
	translateopts(Rec, T, [{<<"orderby">>, [{<<"$natural">>, -1}]}|L]);	
translateopts(Rec, [explain|T], L) ->
	translateopts(Rec, T, [{<<"$explain">>, true}|L]);
translateopts(Rec, [snapshot|T], L) ->
	translateopts(Rec, T, [{<<"$snapshot">>, true}|L]);	
translateopts(undefined, [hint, Hint|T], L) ->
	translateopts(undefined, T, [{<<"$hint">>, [{Hint, 1}]}|L]);
translateopts(Rec, [hint, Hint|T], L) ->
	translateopts(Rec, T, [{<<"$hint">>, {bson, mongodb:encoderec_selector([Hint])}}|L]);
translateopts(_, [], L) ->
	L.

ensureIndex(Rec, Keys) ->
	Bin = mongodb:encode([{plaintext, <<"name">>, mongodb:gen_keyname(Rec, Keys)}, 
			              {plaintext, <<"ns">>, name(element(1,Rec))}, 
			              {<<"key">>, {bson, mongodb:encoderec_selector(Rec, Keys)}}]),
	mongodb:exec_insert(<<DB/binary, ".system.indexes">>, #insert{documents = Bin}).
	
deleteIndexes([_|_] = Collection) ->
	deleteIndexes(list_to_binary(Collection));
deleteIndexes(<<_/binary>> = Collection) ->
	mongodb:exec_cmd(DB, [{plaintext, <<"deleteIndexes">>, Collection}, {plaintext, <<"index">>, <<"*">>}]).

deleteIndex(Rec, Key) ->
	mongodb:exec_cmd(DB,[{plaintext, <<"deleteIndexes">>, atom_to_binary(element(1,Rec), latin1)},
				  		 {plaintext, <<"index">>, mongodb:gen_keyname(Rec,Key)}]).

count([_|_] = Col) ->
	count(list_to_binary(Col));
count(<<_/binary>> = Col) ->
	case mongodb:exec_cmd(DB, [{plaintext, <<"count">>, Col}, {plaintext, <<"ns">>, DB}]) of
		[{<<"n">>, Val}|_] ->
			round(Val);
		_ ->
			false
	end;
count(Col) when is_tuple(Col) ->
	count(atom_to_binary(Col, latin1)).
	

addUser(U, P) when is_binary(U) ->
	addUser(binary_to_list(U),P);
addUser(U, P) when is_binary(P) ->
	addUser(U,binary_to_list(P));
addUser(Username, Password) ->
	save(<<"system.users">>, [{<<"user">>, Username},
							  {<<"pwd">>, bin_to_hexstr(erlang:md5(Username ++ ":mongo:" ++ Password))}]).
	
bin_to_hexstr(Bin) ->
	lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Bin)]).
	
% Runs $cmd. Parameters can be just a string it will be converted into {string,1}
runCmd({_,_} = T) ->
	runCmd([T]);
runCmd([{_,_}|_] = L) ->
	mongodb:exec_cmd(DB, L);
runCmd([_|_] = L) ->
	runCmd([{L,1}]);
runCmd(<<_/binary>> = L) ->
	runCmd(binary_to_list(L)).

repairDatabase() ->
	runCmd([{"repairDatabase", 1}]).
dropDatabase() ->
	runCmd([{"dropDatabase", 1}]).
cloneDatabase(From) when is_list(From); is_binary(From) ->
	runCmd([{"clone", From}]).

dropCollection(C) when is_tuple(C) ->
	dropCollection(atom_to_binary(element(1,C),latin1));
dropCollection(Collection) ->
	runCmd([{"drop", Collection}]).

createCollection(Name) ->
	createCollection(Name, []).
% Options: idindex, noidindex, capped, {size, MaxSizeBytes}, {max, MaxElements}
createCollection(Name, L) when is_tuple(Name) ->
	createCollection(atom_to_binary(element(1,Name), latin1), L);
createCollection(Name, L) ->
	runCmd([{<<"create">>, Name}] ++ translatecolopts(L, [])).

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
	
setProfilingLevel(L) when is_integer(L) ->
	case true of
		_ when L > 0 ->
			createCollection(<<"system.profile">>, [capped, {size, 131072}]);
		_ when L >= 0, L =< 2 ->
			true
	end,
	runCmd([{"profile", L}]).
getProfilingLevel() ->
	runCmd([{"profile", -1}]).
	
	