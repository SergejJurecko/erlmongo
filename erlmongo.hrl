% recindex is index of record in RECTABLE define, it has to be the first record field
% docid is _id in mongodb, it has to be named docid and it has to be the second field in the record
-record(mydoc, {recindex = 1, docid, name, i}).
% A table of records used with mongodb.
-define(RECTABLE, {record_info(fields, mydoc)}).


-export([rec2prop/2, prop2rec/4]).

% Convert record to prop list	
rec2prop(Rec, RecordFields) ->
	loop_rec(RecordFields, 1, Rec, []).

loop_rec([H|T], N, Rec, L) ->
	loop_rec(T, N+1, Rec, [{H, element(N+1, Rec)}|L]);
loop_rec([], _, _, L) ->
	L.

% convert prop list to record
prop2rec(Prop, RecName, DefRec, RecordFields) ->
	loop_fields(erlang:make_tuple(tuple_size(DefRec), RecName), RecordFields, DefRec, Prop, 2).

loop_fields(Tuple, [Field|T], DefRec, Props, N) ->
	case lists:keysearch(Field, 1, Props) of
		{value, {_, Val}} ->
			loop_fields(setelement(N, Tuple, Val), T, DefRec, Props, N+1);
		false ->
			loop_fields(setelement(N, Tuple, element(N, DefRec)), T, DefRec, Props, N+1)
	end;
loop_fields(Tuple, [], _, _, _) ->
	Tuple.
	
	
% mongo	
-define(QUER_OPT_NONE, 0).
-define(QUER_OPT_CURSOR, 2).
-define(QUER_OPT_SLAVEOK, 4).
-define(QUER_OPT_NOTIMEOUT, 16).

-record(cursor, {id, pid, limit = 0}).
-record(update, {upsert = 1, selector = <<>>, document = <<>>}).
-record(insert, {documents = []}).
-record(quer, {ndocs = 1, nskip = 0, quer = <<>>, field_selector = <<>>, opts = ?QUER_OPT_NONE}).
-record(delete, {selector = <<>>}).
-record(killc, {cur_ids = <<>>}).	

