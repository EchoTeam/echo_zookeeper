%%% vim: set ts=4 sts=4 sw=4 expandtab:
-module(echo_zookeeper_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, reconfigure/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {{one_for_one, 10, 10}, specs()}}.

specs() ->
    case application:get_all_key(zookeeper) of
        {ok, ZK} -> 
            application:get_all_key(zookeeper),
            [
		        {
			        echo_zookeeper,
			        {
				        echo_zookeeper,
				        start_link,
				        [
					        [{Host, Port, 30000, 10000} || [Host, Port] <- proplists:get_value(hosts, ZK)],
					        proplists:get_value(chroot, ZK),
					        []
				        ]
			        },
			        permanent, 10000, worker, [echo_zookeeper]
		        }
	        ];
        _ -> lager:error("Config is not available, starting empty"),[]
    end.

reconfigure() ->
    superman:reconfigure_supervisor_init_args(?MODULE, []).
