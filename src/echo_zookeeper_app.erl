-module(echo_zookeeper_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    echo_zookeeper_sup:start_link().

stop(_State) ->
    ok.
