-module(meteor_stream_manager).

-behaviour(gen_server).

-export([start_link/0,
         new/2,
         register/1,
         unregister/1,
         streams/0,
         suicide/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-record(state, {
    streams = #{}    :: map()
}).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

new(Topic, Globals) ->
  case  whereis(meteor_stream_sup) of
    undefined -> meteor_stream_sup:start_link();
    _ -> nop
  end,
  case meteor_stream:find(Topic) of
    not_found ->
      case meteor_stream_sup:start_stream(Topic, Globals) of
      {ok, Stream} -> 
        meteor_stream_manager:register(Topic),
        {ok, Stream} ;
      _ -> 
        {error, unable_to_create_new_stream}
      end;
    Stream ->
      {ok, Stream}
  end.

register(Topic) ->
  case meteor_stream:find(Topic) of
    not_found ->
      {ok, undefined_topic};
    Stream ->
      gen_server:call(?MODULE, {register, Stream, Topic})
  end.

unregister(Topic) ->
  gen_server:call(?MODULE, {unregister, Topic}).

streams() ->
  gen_server:call(?MODULE, list).

suicide() ->
  gen_server:cast(?MODULE, {suicide}).

%%
%% gen_server callbacks
%%

init(_Args) ->
  {ok, #state{streams = #{}}}.

handle_info(synchronize, State) ->
  {noreply, State};

handle_info({'DOWN', _, _, Pid, _}, State = #state{streams = Streams}) ->
  Pred0 = fun(_Topic, {Stream, _Ref}) -> Stream =:= Pid end,
  Pred1 = fun(_Topic, {Stream, _Ref}) -> Stream == Pid end,
  FilteredStreams = maps:filter(Pred0,Streams),
  DownStream = maps:filter(Pred1,Streams),
  Topic = lists:last(maps:keys(DownStream)),
  timer:sleep(32),
  NewStreams = case meteor_stream:find(Topic) of
    not_found ->
      FilteredStreams;
    Stream ->
      Ref = monitor(process, Stream),
      maps:put(Topic, {Stream, Ref}, Streams)
  end,
  NewState = State#state{streams=NewStreams},
  {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

handle_call(list, _From, State = #state{streams = Streams}) ->
  {reply, Streams, State};

handle_call({register, Stream, Topic}, _From, State = #state{streams = Streams}) ->
  Ref = monitor(process, Stream),
  NewStreams = maps:put(Topic, {Stream, Ref}, Streams),
  NewState = State#state{streams=NewStreams},
  {reply, ok, NewState};

handle_call({unregister, Topic}, _From, State = #state{streams = Streams}) ->
  NewState = case maps:get(Topic, Streams, undefined) of
    undefined -> State;
    {_Stream, Ref} ->
        demonitor(process, Ref),
        NewStreams = maps:remove(Topic, Streams),
        State#state{streams=NewStreams}
  end,
  {reply, ok, NewState};

handle_call(_Msg, _From, State) ->
  {reply, {error, {unknown_call, _Msg}}, State}.

handle_cast({suicide}, State) ->
    State = suicide,
    {noreply, State};

handle_cast(_Msg, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.
