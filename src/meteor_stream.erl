-module(meteor_stream).

-behaviour(gen_server).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-export([start_link/2,
         publish/2,
         unsubscribe/1,
         unsubscribe/2,
         subscribe/1,
         subscribe/2,
         flush/1,
         find/1,
         buffer/3,
         debug/1,
         suicide/1,
         batch_ticker/2]).


-record(state, {
    topic               :: meteor:topic(),
    subscribers = []    :: list(),
    messages = []       :: queue:queue(),
    len = 0             :: non_neg_integer(),
    manager_ref         :: term(),
    batchticker_ref     :: term(),
    limit               :: non_neg_integer(),
    microbatch          :: atom(),
    microbatchid        :: non_neg_integer(),
    batchinterval       :: non_neg_integer(),
    batchsize           :: non_neg_integer(),
    ordered             :: atom()
}).

-spec publish(Topic :: meteor:topic() | meteor:stream(),
              Msg :: meteor:message()) -> TS :: meteor:timestamp().

publish(Topic, Msg) ->
  case find(Topic) of
    not_found -> {error, undefined_topic};
    Pid -> gen_server:call(Pid, {publish, Msg})
  end.

-spec subscribe(Topic :: meteor:topic()) -> ok.

subscribe(Topic) ->
  case find(Topic) of
    not_found -> {error, undefined_topic};
    Pid -> gen_server:call(Pid, {subscribe, self()})
  end.

-spec subscribe(Topic :: meteor:topic(), Pid :: pid()) -> ok.

subscribe(Topic, Pid) ->
  case find(Topic) of
    not_found -> {error, undefined_topic};
    Pid -> gen_server:call(Pid, {subscribe, Pid})
  end.

-spec unsubscribe(Topic :: term()) -> ok.

unsubscribe(Topic) ->
  case find(Topic) of
    not_found -> {error, undefined_topic};
    Pid -> gen_server:call(Pid, {unsubscribe, self()})
  end.

-spec unsubscribe(Topic :: meteor:topic(), Pid :: pid()) -> ok.

unsubscribe(Topic, Pid) ->
  case find(Topic) of
    not_found -> {error, undefined_topic};
    StreamPid -> gen_server:call(Pid, {unsubscribe, StreamPid})
  end.

-spec flush(Topic :: term()) -> {ok, Status :: atom()}.

flush(Topic) ->
  case find(Topic) of
    not_found -> {error, undefined_topic};
    Pid -> gen_server:call(Pid, flush)
  end.

-spec find(Topic :: term()) -> Pid :: pid().

find(Pid) when is_pid(Pid) ->
  Pid;
find(Topic) when is_binary(Topic) ->
  TopicAtom = binary_to_atom(Topic, utf8),
  find(TopicAtom);
find(Topic) when is_atom(Topic) ->
  case whereis(Topic) of
    Pid when is_pid(Pid) -> Pid;
    undefined -> not_found
  end;
find(_) ->
  not_found.

-spec start_link(Topic :: meteor:topic(), Globals :: map()) -> Result :: {ok, pid()} | {error, term()}.

start_link(Topic, Globals) ->
  TopicAtom = binary_to_atom(Topic, utf8),
  Globals0 = maps:put(topic, TopicAtom, Globals),
  gen_server:start_link({local, TopicAtom}, ?MODULE, Globals0, []).

-spec debug(Topic  :: meteor:topic() | pid()) 
  -> {reply, {Subscribers :: term(), Msgs :: term(), Len :: term(), Limit :: term()}, State :: term()}.

debug(Topic) ->
  case find(Topic) of
    not_found -> {error, undefined_topic};
    Pid -> gen_server:call(Pid, {debug})
  end.

-spec suicide(Topic  :: meteor:topic() | pid()) -> ok.

suicide(Topic) ->
  case find(Topic) of
    not_found -> {error, undefined_topic};
    Pid -> gen_server:cast(Pid, {debug, suicide})
  end.

-spec buffer(Topic  :: meteor:topic() | pid(),
             Msg    :: meteor:message(),
             Size   :: non_neg_integer()) -> TS :: meteor:timestamp().

buffer(Pid, Type, Size) ->
  gen_server:call(Pid, {buffer, Type, Size}).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

-spec init(Globals :: map()) -> {ok, #state{}}.
init(Globals) ->
    Topic = maps:get(topic, Globals),
    Microbatch = maps:get(microbatch, Globals, false),
    Batchinterval =maps:get(batchinterval, Globals, 128),
    State0 = #state{topic = Topic,
                    messages = queue:new(),
                    len = 0,
                    limit = maps:get(limit, Globals, 256),
                    manager_ref = undefined,
                    batchticker_ref = undefined,
                    microbatch = Microbatch,
                    microbatchid = 0,
                    batchinterval = maps:get(batchinterval, Globals, 32),
                    batchsize = maps:get(batchsize, Globals, 256),
                    ordered = maps:get(ordered, Globals, true)
                },
    case whereis(meteor_stream_manager) of
      MPid when is_pid(MPid) ->
        MonitorMRef = monitor(process, MPid),
        case Microbatch of
          true ->
            case spawn(meteor_stream, batch_ticker, [self(), Batchinterval]) of
              TPid when is_pid(TPid) ->
                MonitorTRef = monitor(process, TPid),
                {ok, State0#state{manager_ref = MonitorMRef, batchticker_ref = MonitorTRef}};
              undefined ->
                {stop, unable_to_spawn_batchticker}
            end;
          false ->
            {ok, State0#state{manager_ref = MonitorMRef}}
        end;
      undefined -> 
        {stop, meteor_stream_manager_not_running}
    end.

handle_call({publish, Msg}, _From, State) ->
  State0 = handle_msg(State, Msg),
  {NewState, Status} = handle_publish(State0),
  {reply, {ok, Status}, NewState};

handle_call(flush, _From, State) ->
  {NewState, Status} = handle_flush(State),
  {reply, {ok, Status}, NewState};

handle_call({subscribe, Pid}, _From, State = #state{subscribers = Subscribers}) ->
    Ref = erlang:monitor(process, Pid),
    NewList = [{Pid,Ref} | Subscribers],
    State0 = State#state{subscribers = NewList},
    {NewState, _Status} = handle_publish(State0),
    {reply, {ok,Ref}, NewState};

handle_call({unsubscribe, Pid}, _From, State = #state{subscribers = Subscribers}) ->
    Remain = remove(Pid, Subscribers),
    NewState = State#state{subscribers = Remain},
    {reply, ok, NewState};

handle_call({debug}, _From, State = #state{messages = Msgs,
                                           len = Len,
                                           limit = Limit,
                                           subscribers = Subscribers}) ->
    {reply, {Subscribers,Msgs,Len,Limit}, State};

handle_call(_Msg, _From, State) ->
    {reply, {error, {unknown_call, _Msg}}, State}.

handle_cast({debug, suicide}, State) ->
    State = suicide,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MonitorRef, _, Pid, _}, State = #state{subscribers = Subscribers, 
                                                            manager_ref = MRef,
                                                            batchticker_ref = TRef,
                                                            batchinterval = Batchinterval}) ->
    case MonitorRef of
      MRef ->
        self() ! locate_manager, 
        NewState = State#state{manager_ref = undefined},
        {noreply, NewState};
      TRef ->
        case spawn(meteor_stream, batch_ticker, [self(), Batchinterval]) of
          TPid when is_pid(TPid) ->
            MonitorTRef = monitor(process, TPid),
            NewState = State#state{batchticker_ref = MonitorTRef},
            {noreply, NewState}; 
          undefined ->
              NewState = State#state{batchticker_ref = undefined},
              {stop, unable_to_spawn_batchticker, NewState}
        end;     
      _ ->
        Remain = remove(Pid, Subscribers),
        NewState = State#state{subscribers = Remain},
        {noreply, NewState}
    end;

handle_info(locate_manager, State = #state{topic = Topic}) ->
    NewState = case locate_manager(State, Topic) of
        {ok, StateOk} -> 
          StateOk;
        {ko, StateKo} ->
          self() ! locate_manager, 
          StateKo
    end,
    {noreply, NewState};

handle_info(flush, State) ->
  {NewState, _Status} = handle_flush(State),
  {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================
handle_msg(State, Messages) ->
  queuein(State, Messages).

handle_publish( State = #state{topic = Topic,
                               messages = Messages,
                               len = Len,
                               limit = Limit,
                               microbatch = IsBatch,
                               microbatchid = BatchId,
                               batchsize = BatchSize,
                               subscribers = Subscribers}) ->

  case length(Subscribers) of
    0 ->
      if
        Len >= Limit ->
          NbMsgToRemove = queue:len(Messages) - Limit,
          List0 = queue:to_list(Messages),
          {_, List} = lists:split(NbMsgToRemove, List0),
          NewQueue = queue:from_list(List),
          {State#state{messages=NewQueue, len=Len-NbMsgToRemove}, queued_no_subscribers};
        true ->
          {State, queued_no_subscribers}
      end;
    _ -> 
      case IsBatch of
        true -> 
          if 
            Len >= BatchSize ->
              microbatch(Topic, Messages, Subscribers, BatchId),
              {State#state{messages=queue:new(), len=0, microbatchid=BatchId+1}, delivered};
            true ->
              {State, queued_microbatch_mode}
          end;
        false ->
          deliver(Topic, State#state.messages, Subscribers),
          {State#state{messages=queue:new(), len=0}, delivered}
      end
  end.

handle_flush( State = #state{topic = Topic,
                             messages = Messages,
                             len = Len,
                             microbatch = IsBatch,
                             microbatchid = BatchId,
                             subscribers = Subscribers}) ->
  if
    Len > 0 ->
      case length(Subscribers) of
        0 -> 
          {State, no_subscribers};
        _ -> 
          case IsBatch of
            true ->
              microbatch(Topic, Messages, Subscribers, BatchId),
              {State#state{messages=queue:new(), len=0, microbatchid=BatchId+1}, delivered};
            false ->
              deliver(Topic, State#state.messages, Subscribers),
              {State#state{messages=queue:new(), len=0}, delivered}
          end
      end;
    true ->
      {State, queue_empty}
  end.

queuein(State = #state{messages = Msgs, len = Len}, Messages) when is_list(Messages) ->
  QueueFrom = queue:from_list(Messages),
  NewQueue = queue:join(Msgs, QueueFrom),
  State#state{messages=NewQueue, len=Len+queue:len(QueueFrom)};
queuein(State = #state{messages = Msgs, len = Len}, Message) ->
  QueueIn = queue:in(Message, Msgs),
  State#state{messages=QueueIn, len=Len+1}.

microbatch(Topic, Messages, Subscribers, Id) when is_list(Messages) ->
  [Sub ! {stream, Topic, {microbatch, Id, Messages}} || {Sub, _Ref} <- Subscribers];
microbatch(Topic, Messages, Subscribers, Id) ->
  case queue:is_queue(Messages) of
    true -> 
      List = queue:to_list(Messages),
      microbatch(Topic, List, Subscribers, Id);
    _ ->
      nop
  end.

deliver(Topic, Messages, Subscribers) when is_list(Messages) ->
  [deliver(Topic, Msg, Subscribers) || Msg <- Messages];
deliver(Topic, Messages, Subscribers) ->
  case queue:is_queue(Messages) of
    true ->
      List = queue:to_list(Messages),
      [ deliver(Topic, Msg, Subscribers) || Msg <- List];
    _ ->
      [Sub ! {stream, Topic, Messages} || {Sub, _Ref} <- Subscribers]
  end.

remove(Pid, Subscribers) ->
    {Delete, Remain} = lists:partition(fun({P,_Ref}) -> P == Pid end, Subscribers),
    [erlang:demonitor(Ref) || {_Pid,Ref} <- Delete],
    Remain.

locate_manager(State, Topic) ->
  case  whereis(meteor_stream_manager) of
    Pid when is_pid(Pid) ->
      ManagerRef = monitor(process, Pid),
      meteor_stream_manager:register(Topic),
      NewState = State#state{manager_ref = ManagerRef},
      {ok, NewState};
    _ -> 
      {ko, State}
    end.

batch_ticker(TopicPid, Interval) ->
  timer:sleep(Interval),
  TopicPid ! flush,
  batch_ticker(TopicPid, Interval).
