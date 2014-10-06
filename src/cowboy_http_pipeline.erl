-module (cowboy_http_pipeline).
-behaviour (cowboy_sub_protocol).
-export([upgrade/4]).

-type req_props() :: cowboy_http_pipeline_response_serializer:req_props().
-type handler_opts() :: cowboy_http_handler:opts().

-callback handle( req_props(), handler_opts() ) ->
	  {ok, cowboy:http_status(), cowboy:http_headers(), iodata()}
	| {ok, cowboy:http_status(), cowboy:http_headers()}
	| {ok, cowboy:http_status()}.

upgrade( Req0, Env0, Handler, HandlerOpts ) ->
	{ok, ResponseSerializer} = get_response_serializer(),
	{ok, _Body, Req1} = cowboy_req:body( Req0 ),
	{ok, _ReqHandler} = cowboy_http_pipeline_response_serializer:start_handler( ResponseSerializer, Req1, Handler, HandlerOpts ),
	Req2 = cowboy_req:set( [{resp_state, done}], Req1 ),
	Env1 = [ {result, ok} | Env0 ],
	{ok, Req2, Env1}.


-define(key_response_serializer, cowboy_http_pipeline_response_serializer).
get_response_serializer() ->
	case erlang:get( ?key_response_serializer ) of
		undefined ->
			{ok, ResponseSerializer} = cowboy_http_pipeline_response_serializer:start_link( self() ),
			undefined = erlang:put(?key_response_serializer, ResponseSerializer ),
			{ok, ResponseSerializer};
		ResponseSerializer when is_pid( ResponseSerializer ) ->
			{ok, ResponseSerializer}
	end.
