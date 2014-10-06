-module (cowboy_http_pipeline_worker).
-export ([start_link/5]).
-export ([enter_loop/5]).

start_link( ResponseSerializer, ReqID, ReqProps, Handler, HandlerOpts ) ->
	proc_lib:start_link( ?MODULE, enter_loop, [ ResponseSerializer, ReqID, ReqProps, Handler, HandlerOpts ] ).

enter_loop( ResponseSerializer, ReqID, ReqProps, Handler, HandlerOpts ) ->
	ok = proc_lib:init_ack({ok, self()}),
	try case Handler:handle( ReqProps, HandlerOpts ) of
		{ok, HttpStatus, HttpHeaders, Body} -> response( ResponseSerializer, ReqID, HttpStatus, HttpHeaders, Body );
		{ok, HttpStatus, HttpHeaders} -> response( ResponseSerializer, ReqID, HttpStatus, HttpHeaders, <<>> );
		{ok, HttpStatus} -> response( ResponseSerializer, ReqID, HttpStatus, [], <<>> )
	end catch
		Error:Reason ->
			response( ResponseSerializer, ReqID, 500, [], <<>> ),
			erlang:exit({Error, Reason, erlang:get_stacktrace()})
	end.

response( ResponseSerializer, ReqID, HttpStatus, HttpHeaders, HttpBody ) ->
	cowboy_http_pipeline_response_serializer:response( ResponseSerializer, ReqID, HttpStatus, HttpHeaders, HttpBody ).
