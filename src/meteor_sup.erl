-module(meteor_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%%
%% External API
%%

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%
%% supervisor callbacks
%%
init(_Args) ->
  SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
  ChildSpec = #{id => meteor_stream_manager,
                modules => [meteor_stream_manager],
                start => {meteor_stream_manager, start_link, []},
                restart => permanent,
                shutdown => brutal_kill,
                type => worker},
  {ok, {SupFlags, [ChildSpec]}}.
