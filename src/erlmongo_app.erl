-module(erlmongo_app).
-behavior(application).
-export([start/2, stop/1]).


start(_Type, _Args) ->
	mongodb_supervisor:start_link().


stop(_State) ->
	ok.
