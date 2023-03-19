-module(meteor_stream_sup).
-behaviour(supervisor).


-export([start_link/0]).
-export([init/1]).

-export([start_stream/2,
         stop_stream/1]).


start_link() ->
  supervisor:start_link({local,?MODULE}, ?MODULE, []).


start_stream(Topic, Globals) ->
  ChildSpec = #{id => Topic,
                modules => [meteor_stream],
                start => {meteor_stream, start_link, [Topic, Globals]},
                restart => permanent,
                shutdown => brutal_kill,
                type => worker},
  supervisor:start_child(meteor_stream_sup, ChildSpec).

stop_stream(Topic) ->
  supervisor:terminate_child(meteor_stream_sup, Topic),
  supervisor:delete_child(meteor_stream_sup, Topic).


%%
%% supervisor callbacks
%%
init(_Args) ->
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.