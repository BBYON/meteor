# meteor
Meteor - Messaging pubsub behavior for Orbital


cd("C:\\Users\\JTriolet\\Repository\\BBYON-HQ\\Milkywai-HQ\\meteor\\ebin").
meteor_stream_sup:start_link().
meteor_stream_sup:start_stream(<<"stream.plop">>).
meteor_stream:debug(<<"stream.plop">>).
meteor_stream:publish(<<"stream.plop">>, <<"hello!">>).
meteor_stream:subscribe(<<"stream.plop">>).
meteor_stream:unsubscribe(<<"stream.plop">>).

meteor_stream_sup:start_stream(<<"stream.tralala">>).


meteor_stream:suicide(<<"stream.plop">>).

meteor_stream_manager:start_link().

meteor_stream_manager:streams().


meteor_sup:start_link().

meteor:start_link().
meteor:new(<<"stream.plop">>).
meteor:new(<<"stream.hello">>).
meteor:new(<<"stream.tralala">>).
meteor:publish(<<"stream.plop">>, <<"hello!">>).
meteor:subscribe(<<"stream.plop">>).
meteor:unsubscribe(<<"stream.plop">>).

meteor_stream_manager:streams().
meteor_stream_manager:suicide().

Globals = #{microbatch => true, batchsize => 8}.
meteor:new(<<"stream.tralala">>, Globals).

meteor_stream:flush(<<"stream.tralala">>).

meteor:publish(<<"stream.tralala">>, [1,2,3,4]).
meteor:subscribe(<<"stream.tralala">>).
meteor:publish(<<"stream.plop">>, [1,2,3,4,5,6,7,8,9,10]).
