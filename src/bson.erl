-module(bson).
-export([encode/1, decode/1, encode/2, decode/2, dec2hex/2, hex2dec/2]).
-export([recfields/1, encoderec/1, encode_findrec/1, encoderec_selector/2,
  decoderec/2, gen_keyname/2, gen_prop_keyname/2, recoffset/1]).
-include_lib("erlmongo.hrl").

encode(undefined) ->
	<<>>;
encode(<<>>) ->
	<<>>;
encode(Items) ->
	encode(Items, default).

encode(#{} = Items, S) ->
  encode(maps:to_list(Items), S);
encode(Items, Style) ->
	Bin = lists:foldl(fun(Item, B) -> <<B/binary, (encode_element(Item, Style))/binary>> end, <<>>, Items),
	<<(byte_size(Bin)+5):32/little-signed, Bin/binary, 0:8>>.

% mochijson behaviour
encode_element({Name, [{_,_}|_] = Items}, mochijson) ->
	Binary = encode(Items, mochijson),
	<<3, Name/binary, 0, Binary/binary>>;

encode_element({Name, Items}, mochijson) when is_list(Items) ->
	<<4, Name/binary, 0, (encarray([], Items, 0, mochijson))/binary>>;

encode_element(A, _Style) ->
	encode_element(A). %fallback

% default behaviour
encode_element({[_|_] = Name, Val}) ->
	encode_element({list_to_binary(Name),Val});
encode_element({'or',[{_,_}|_] = L}) ->
	encode_element({<<"$or">>,{array,[[Obj] || Obj <- L]}});
encode_element({Name, Val}) when is_atom(Name) ->
	encode_element({atom_to_binary(Name, utf8),Val});
encode_element({<<_/binary>> = Name, #{} = Items}) ->
	Binary = encode(maps:to_list(Items)),
	<<3, Name/binary, 0, Binary/binary>>;
encode_element({<<_/binary>> = Name, [O|_] = Items}) when is_tuple(O) ->
	Binary = encode(Items),
	<<3, Name/binary, 0, Binary/binary>>;
encode_element({Name, []}) ->
	<<2, Name/binary, 0, 1:32/little-signed, 0>>;
encode_element({Name, [M|_] = Items}) when is_map(M) ->
  	<<4, Name/binary, 0, (encarray([], Items, 0))/binary>>;
encode_element({<<_/binary>> = Name, [_|_] = Value}) ->
	ValueEncoded = encode_cstring(Value),
	<<2, Name/binary, 0, (byte_size(ValueEncoded)):32/little-signed, ValueEncoded/binary>>;
encode_element({Name, <<_/binary>> = Value}) ->
	ValueEncoded = encode_cstring(Value),
	<<2, Name/binary, 0, (byte_size(ValueEncoded)):32/little-signed, ValueEncoded/binary>>;
encode_element({Name, true}) ->
	<<8, Name/binary, 0, 1:8>>;
encode_element({Name, false}) ->
	<<8, Name/binary, 0, 0:8>>;
encode_element({Name, null}) ->
	<<10, Name/binary, 0>>;
encode_element({Name,min}) ->
	<<255, Name/binary, 0>>;
encode_element({Name,max}) ->
	<<127, Name/binary, 0>>;
encode_element({Name, Value}) when is_atom(Value) ->
	ValueEncoded = encode_cstring(atom_to_binary(Value,utf8)),
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
encode_element({Name, {binary, Data}}) ->
  encode_element({Name, {binary,0, Data}});
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
encode_element({Name, {{Yr,Mo,Dy},{Hr,Mi,Sd}}=Value})
		when is_integer(Yr),is_integer(Mo),is_integer(Dy), is_integer(Hr),is_integer(Mi),is_integer(Sd) ->
	%% 62167219200 == calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
	Millis = (calendar:datetime_to_gregorian_seconds(Value)-62167219200) * 1000,
	<<9, Name/binary, 0, Millis:64/little-signed>>;
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


encarray(L, Lst, N) -> %fallback
	encarray(L, Lst, N, default).

encarray(L, [H|T], N, Style) ->
	encarray([{list_to_binary(integer_to_list(N)), H}|L], T, N+1, Style);
encarray(L, [], _, Style) ->
	encode(lists:reverse(L), Style).

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
	decode(proplist,Bin).
decode(Format,Bin) ->
	% io:format("Decoding ~p~n", [Bin]),
	decode(Format,Bin,[]).
decode(Format,<<_Size:32, Bin/binary>>, L) ->
  	{BSON, Rem} = decode_next(Format,Bin, []),
	decode(Format, Rem,[BSON|L]);
decode(_Format,<<>>, L) ->
	lists:reverse(L).

decode_next(proplist,<<>>, Accum) ->
  	{lists:reverse(Accum), <<>>};
decode_next(map,<<>>, Accum) ->
  	{maps:from_list(Accum), <<>>};
decode_next(proplist,<<0:8, Rest/binary>>, Accum) ->
	{lists:reverse(Accum), Rest};
decode_next(map,<<0:8, Rest/binary>>, Accum) ->
	{maps:from_list(Accum), Rest};
decode_next(Format,<<Type:8/little, Rest/binary>>, Accum) ->
  	{Name, EncodedValue} = decode_cstring(Rest, <<>>),
% io:format("Decoding ~p~n", [Type]),
  	{Value, Next} = decode_value(Format,Type, EncodedValue),
  	decode_next(Format,Next, [{Name, Value}|Accum]).

decode_cstring(<<>>, _) ->
	throw({invalid_cstring});
decode_cstring(<<0:8, Rest/binary>>, Acc) ->
	{Acc, Rest};
decode_cstring(<<C:8, Rest/binary>>, Acc) ->
	decode_cstring(Rest, <<Acc/binary, C:8>>).

decode_value(_F,7, <<OID:12/binary,Rest/binary>>) ->
  	{{oid, dec2hex(<<>>, OID)}, Rest};
decode_value(_F,16, <<Integer:32/little-signed, Rest/binary>>) ->
	{Integer, Rest};
decode_value(_F,18, <<Integer:64/little-signed, Rest/binary>>) ->
	{Integer, Rest};
decode_value(_F,1, <<Double:64/little-signed-float, Rest/binary>>) ->
	{Double, Rest};
decode_value(_F,2, <<Size:32/little-signed, Rest/binary>>) ->
	StringSize = Size-1,
	case Rest of
		<<String:StringSize/binary, 0:8, Remain/binary>> ->
			{String, Remain};
		<<String:Size/binary, Remain/binary>> ->
			{String,Remain}
	end;
decode_value(Format,3, <<Size:32/little-signed, Rest/binary>> = Binary) when byte_size(Binary) >= Size ->
  	decode_next(Format,Rest, []);
decode_value(Format,4, <<Size:32/little-signed, Data/binary>> = Binary) when byte_size(Binary) >= Size ->
  	{Array, Rest} = decode_next(Format,Data, []),
		case Format of
			proplist ->
				{{array,[Value || {_Key, Value} <- Array]}, Rest};
			map ->
				Fun = fun({A,_},{B,_}) -> binary_to_integer(A) =< binary_to_integer(B) end,
				{[Value || {_Key, Value} <- lists:sort(Fun,maps:to_list(Array))], Rest}
		end;
decode_value(_F,5, <<_Size:32/little-signed, 2:8/little, BinSize:32/little-signed, BinData:BinSize/binary-little-unit:8, Rest/binary>>) ->
  	{{binary, 2, BinData}, Rest};
decode_value(_F,5, <<Size:32/little-signed, SubType:8/little, BinData:Size/binary-little-unit:8, Rest/binary>>) ->
  	{{binary, SubType, BinData}, Rest};
decode_value(_F,6, _Binary) ->
  	throw(encountered_undefined);
decode_value(_F,8, <<0:8, Rest/binary>>) ->
	{false, Rest};
decode_value(_F,8, <<1:8, Rest/binary>>) ->
  	{true, Rest};
decode_value(_F,9, <<Millis:64/little-signed, Rest/binary>>) ->
	UnixTime = Millis div 1000,
  	MegaSecs = UnixTime div 1000000,
  	Secs = UnixTime - (MegaSecs * 1000000),
  	MicroSecs = (Millis - (UnixTime * 1000)) * 1000,
  	{{MegaSecs, Secs, MicroSecs}, Rest};
decode_value(_F,10, Binary) ->
  	{null, Binary};
decode_value(_F,11, Binary) ->
  	{Expression, RestWithFlags} = decode_cstring(Binary, <<>>),
  	{Flags, Rest} = decode_cstring(RestWithFlags, <<>>),
  	{{regex, Expression, Flags}, Rest};
decode_value(_F,12, <<Size:32/little-signed, Data/binary>> = Binary) when size(Binary) >= Size ->
	{NS, RestWithOID} = decode_cstring(Data, <<>>),
	{{oid, OID}, Rest} = decode_value(_F,7, RestWithOID),
	{{ref, NS, OID}, Rest};
decode_value(_F,13, <<_Size:32/little-signed, Data/binary>>) ->
	{Code, Rest} = decode_cstring(Data, <<>>),
	{{code, Code}, Rest};
decode_value(_F,14, <<_Size:32/little-signed, Data/binary>>) ->
	{Code, Rest} = decode_cstring(Data, <<>>),
	{{symbol, Code}, Rest};
decode_value(_F,15, <<ComplSize:32/little, StrBSize:32/little,Rem/binary>>) ->
	StrSize = StrBSize - 1,
	ScopeSize = ComplSize - 8 - StrBSize,
	<<Code:StrSize/binary, _, Scope:ScopeSize/binary,Rest/binary>> = Rem,
	{{code,Code,decode(_F,Scope)}, Rest};
decode_value(_F,17, <<Integer:64/little-signed, Rest/binary>>) ->
	{Integer, Rest};
decode_value(_F,255, Rest) ->
	{min, Rest};
decode_value(_F,127, Rest) ->
	{max, Rest};
decode_value(_F,18, <<Integer:32/little-signed, Rest/binary>>) ->
	{Integer, Rest}.

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
			encoderec(NameRec, Type, Rec, T, N+1, <<Bin/binary, (encoderec(Dom, flat, SubRec, SubFields, recoffset(FieldName), <<>>))/binary>>);
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
					case NameRec of
						<<>> ->
							Dom = <<"_id">>;
						_ ->
							Dom = <<NameRec/binary, "._id">>
					end,
					case Val of
						{oid, _} ->
							encoderec(NameRec, Type,Rec, T, N+1, <<Bin/binary, (encode_element({Dom, {oid, Val}}))/binary>>);
						_ ->
							encoderec(NameRec, Type,Rec, T, N+1, <<Bin/binary, (encode_element({Dom, Val}))/binary>>)
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
		true when T == [] ->
			Add = <<(list_to_binary(integer_to_list(KeyVal)))/binary>>;
		true ->
			Add = <<(list_to_binary(integer_to_list(KeyVal)))/binary,"_">>;
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
		true when Keys == [] ->
			Add = <<(list_to_binary(integer_to_list(KeyVal)))/binary>>;
		true ->
			Add = <<(list_to_binary(integer_to_list(KeyVal)))/binary,"_">>;
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
	{_Value, Remain} = decode_value(proplist,Type, ValRem),
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
					{Value, Remain} = decode_value(proplist,Type, ValRem),
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
