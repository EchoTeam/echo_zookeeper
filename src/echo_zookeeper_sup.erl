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
    {ok, Hosts} = application:get_env(zookeeper, hosts),
    {ok, Chroot} = application:get_env(zookeeper, chroot), 
	[
		{
			echo_zookeeper,
			{
				echo_zookeeper,
				start_link,
				[
					[{Host, Port, 30000, 10000} || [Host, Port] <- Hosts],
					Chroot,
					[]
				]
			},
			permanent, 10000, worker, [echo_zookeeper]
		}
	].

reconfigure() ->
    superman:reconfigure_supervisor_init_args(?MODULE, []).
