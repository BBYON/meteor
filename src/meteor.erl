-module(meteor).

-export([new/1,
         new/2,
         publish/2,
         subscribe/1,
         subscribe/2,
         unsubscribe/1,
         unsubscribe/2,
         % multi_fetch/2,
         % multi_fetch/3,
         start_link/0]).

-type stream() :: pid().
-type topic() :: binary().
-type message() :: binary().
-type timestamp() :: non_neg_integer().

-export_type([stream/0, message/0, timestamp/0, topic/0]).

-spec start_link() -> ok.
start_link() ->
  meteor_sup:start_link().

-spec new(Topic :: topic()) -> {ok, {Topic :: topic(), Stream :: stream()}} .
new(Topic) ->
  new(Topic, #{}).

-spec new(Topic :: topic(), Globals :: map()) -> {ok, {Topic :: topic(), Stream :: stream()}} .
new(Topic, Globals) ->
  meteor_stream_manager:new(Topic, Globals).


-spec publish(Topic :: topic(), Msg :: message()) -> TS :: timestamp().
publish(Topic, Msg) ->
  meteor_stream:publish(Topic, Msg).

-spec subscribe(Topic :: topic()) -> ok.
subscribe(Topic) ->
  meteor_stream:subscribe(Topic).

-spec subscribe(Topic :: topic(), Pid :: pid()) -> ok.
subscribe(Topic, Pid) ->
  meteor_stream:subscribe(Topic, Pid).

-spec unsubscribe(Topic :: topic()) -> ok.
unsubscribe(Topic) ->
  meteor_stream:unsubscribe(Topic).

-spec unsubscribe(Topic :: topic(), Pid :: pid()) -> ok.
unsubscribe(Topic, Pid) ->
  meteor_stream:unsubscribe(Topic, Pid).

%-spec multi_fetch([Topic :: topic()], TS :: timestamp()) ->
%  {ok, LastTS :: timestamp(), [Msg :: term()]}.
%multi_fetch(Topics, TS) ->
%  meteor_stream:multi_fetch(Topics, TS).

%-spec multi_fetch([Topic :: topic()], TS :: timestamp(), Timeout :: non_neg_integer()) ->
%  {ok, LastTS :: timestamp(), [Msg :: message()]}.
%multi_fetch(Topics, TS, Timeout) ->
%  meteor_stream:multi_fetch(Topics, TS, Timeout).
