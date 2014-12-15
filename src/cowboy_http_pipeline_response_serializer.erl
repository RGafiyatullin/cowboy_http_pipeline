-module (cowboy_http_pipeline_response_serializer).
-behaviour (gen_server).
-export ([start_link/1]).
-export ([
		start_handler/4,
		response/5
	]).
-export ([
		init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3
	]).
-define(max_pipeline_len, 5).
-define(start_handler( Req, ReqProps, Handler, HandlerOpts ), {start_handler, Req, ReqProps, Handler, HandlerOpts}).
-define(response(ReqID, HttpStatus, HttpHeaders, HttpBody), {response, ReqID, HttpStatus, HttpHeaders, HttpBody}).

start_link( ConnPid ) ->
	gen_server:start_link( ?MODULE, {ConnPid}, [] ).

start_handler( ResponseSerializer, Req0, Handler, HandlerOpts ) ->
	{Req1, ReqProps} = lists:foldl(
		fun( F, {ReqIn, ReqPropsIn} ) ->
			{Field, Value, ReqOut} = F(ReqIn),
			ReqPropsOut = [ {Field, Value} | ReqPropsIn ],
			{ReqOut, ReqPropsOut}
		end,
		{Req0, []}, [
				fun req_get_method/1,
				fun req_get_qs/1,
				fun req_get_headers/1,
				fun req_get_body/1,
				fun req_get_bindings/1,
				fun req_get_path_info/1
			]),
	gen_server:call( ResponseSerializer, ?start_handler( Req1, ReqProps, Handler, HandlerOpts ) ).

response( ResponseSerializer, ReqID, HttpStatus, HttpHeaders, HttpBody ) ->
	gen_server:call( ResponseSerializer, ?response(ReqID, HttpStatus, HttpHeaders, HttpBody) ).

-record(req_worker, {
		req_id :: integer(),
		req :: cowboy_req:req(),
		worker_pid :: pid()
	}).
-record(response, {
		code :: cowboy:http_status(),
		headers :: cowboy:http_headers(),
		body :: iodata()
	}).
-record(pending_request, {
		req :: cowboy_req:req(),
		req_props :: [{atom(), term()}],
		handler :: atom(),
		handler_opts :: term(),
		gen_reply_to :: {pid(), reference()}
	}).
-record(s, {
		conn_pid :: pid(),
		worker_sup :: pid(),
		req_queue = queue:new() :: queue:queue( #req_worker{} ),
		resp_map = orddict:new() :: orddict:orddict( #response{} ),
		next_req_id = 0 :: non_neg_integer(),
		max_pipeline_len = ?max_pipeline_len :: pos_integer(),
		pending_request = undefined :: #pending_request{} | undefined
	}).
init( {ConnPid} ) ->
	ResponseSerializer = self(),
	_ConnMonRef = erlang:monitor( process, ConnPid ),
	{ok, WorkerSup} = simplest_one_for_one:start_link(
		{ cowboy_http_pipeline_worker, start_link, [ ResponseSerializer ] } ),
	{ok, #s{
			conn_pid = ConnPid,
			worker_sup = WorkerSup
		}}.

handle_call( ?start_handler( Req, ReqProps, Handler, HandlerOpts ), GenReplyTo, State ) ->
	handle_call_start_handler( Req, ReqProps, Handler, HandlerOpts, GenReplyTo, State );

handle_call( ?response( ReqID, HttpStatus, HttpHeaders, HttpBody ), GenReplyTo, State ) ->
	handle_call_response( ReqID, HttpStatus, HttpHeaders, HttpBody, GenReplyTo, State );

handle_call( Unexpected, GenReplyTo, State ) ->
	error_logger:warning_report([ ?MODULE, handle_call,
		{unexpected, Unexpected}, {gen_reply_to, GenReplyTo} ]),
	{reply, badarg, State}.

handle_cast( Unexpected, State ) ->
	error_logger:warning_report([ ?MODULE, handle_cast, {unexpected, Unexpected} ]),
	{noreply, State}.

handle_info( {'DOWN', _MonRef, process, ConnPid, _}, State = #s{ conn_pid = ConnPid } ) ->
	% log([?MODULE, handle_info, {conn_down, ConnPid}]),
	{stop, {shutdown, conn_down}, State};

handle_info( Unexpected, State ) ->
	error_logger:warning_report([ ?MODULE, handle_info, {unexpected, Unexpected} ]),
	{noreply, State}.

terminate( _Reason, _State ) ->	 ok.
code_change( _OldVsn, State, _Extra ) -> {ok, State}.



handle_call_response( ReqID, HttpStatus, HttpHeaders, HttpBody, _GenReplyTo, State0 = #s{ req_queue = RqQ0, resp_map = RsM0 } ) ->
	Response = #response{ code = HttpStatus, headers = HttpHeaders, body = HttpBody },
	{RqQ1, RsM1} = maybe_flush_responses( RqQ0, orddict:store( ReqID, Response, RsM0 ) ),
	State1 = State0 #s{ req_queue = RqQ1, resp_map = RsM1 },
	State2 = maybe_start_pending_request( State1 ),
	{reply, ok, State2}.

handle_call_start_handler(
	Req, ReqProps, Handler,
	HandlerOpts, GenReplyTo,
	State0 = #s{
		pending_request = undefined
	}
) ->
	case max_pipeline_len_reached( State0 ) of
		false ->
			{ok, WorkerPid, State1} = do_start_handler( Req, ReqProps, Handler, HandlerOpts, State0 ),
			{reply, {ok, WorkerPid}, State1};
		true ->
			State1 = do_pend_request( Req, ReqProps, Handler, HandlerOpts, GenReplyTo, State0 ),
			{noreply, State1}
	end.

max_pipeline_len_reached( #s{ req_queue = RqQ, max_pipeline_len = MPL } ) ->
	queue:len( RqQ ) >= MPL.

do_pend_request( Req, ReqProps, Handler, HandlerOpts, GenReplyTo, State0 = #s{ pending_request = undefined } ) ->
	PendingRequest = #pending_request{
			req = Req, req_props = ReqProps,
			handler = Handler, handler_opts = HandlerOpts,
			gen_reply_to = GenReplyTo
		},
	State0 #s{ pending_request = PendingRequest }.

do_start_handler(
	Req, ReqProps, Handler,
	HandlerOpts,
	State0 = #s{
		worker_sup = WorkerSup,
		req_queue = ReqQueue0
	}
) ->
	{ReqID, State1} = next_req_id( State0 ),
	{ok, WorkerPid} = start_link_worker( WorkerSup, ReqID, ReqProps, Handler, HandlerOpts ),
	ReqWorker = #req_worker{
			req_id = ReqID,
			req = Req,
			worker_pid = WorkerPid
		},
	ReqQueue1 = queue:in( ReqWorker, ReqQueue0 ),
	State2 = State1 #s{ req_queue = ReqQueue1 },
	{ok, WorkerPid, State2}.

maybe_start_pending_request( State = #s{ pending_request = undefined } ) -> State;
maybe_start_pending_request( State = #s{ pending_request = #pending_request{} } ) ->
	case max_pipeline_len_reached( State ) of
		true -> State;
		false ->
			do_start_pending_request( State )
	end.

do_start_pending_request(
	State0 = #s{ pending_request =
		#pending_request{
			req = Req, req_props = ReqProps,
			handler = Handler, handler_opts = HandlerOpts,
			gen_reply_to = GenReplyTo
		}
	}
) ->
	State1 = State0 #s{ pending_request = undefined },
	{ok, WorkerPid, State2} = do_start_handler( Req, ReqProps, Handler, HandlerOpts, State1 ),
	ReplyWith = {ok, WorkerPid},
	_Ignored = gen_server:reply( GenReplyTo, ReplyWith ),
	State2.



maybe_flush_responses( RqQ0, RsM0 ) ->
	% log([?MODULE, maybe_flush_responses,
	% 	{rqq, [ ID || #req_worker{ req_id = ID } <- queue:to_list( RqQ0 ) ]},
	% 	{rsm, [ ID || {ID, #response{}} <- lists:sort(orddict:to_list( RsM0 )) ]}]),
	case queue:peek( RqQ0 ) of
		empty ->
			% log([?MODULE, maybe_flush_responses, {rq_q, empty}]),
			0 = orddict:size( RsM0 ),
			{RqQ0, RsM0};
		{value, #req_worker{ req_id = ReqID, req = CowboyReq }} ->
			% log([?MODULE, maybe_flush_responses, {rq_q_peek_id, ReqID}]),
			case orddict:find( ReqID, RsM0 ) of
				error ->
					% log([?MODULE, maybe_flush_responses, {no_rs_match, ReqID}]),
					{RqQ0, RsM0};
				{ok, ResponseMatched} ->
					% log([?MODULE, maybe_flush_responses, {rs_matched, ReqID}]),
					RqQ1 = queue:drop( RqQ0 ),
					RsM1 = orddict:erase( ReqID, RsM0 ),
					ok = flush_single_response( CowboyReq, ResponseMatched ),

					maybe_flush_responses( RqQ1, RsM1 )
			end
	end.

flush_single_response( CowboyReq, #response{ code = HttpStatus, headers = HttpHeaders, body = HttpBody } ) ->
	{ok, _} = cowboy_req:reply( HttpStatus, HttpHeaders, HttpBody, CowboyReq ),
	ok.

req_get_method( R0 ) ->
	{ Method, R1 } = cowboy_req:method( R0 ),
	{ method, Method, R1 }.

req_get_qs( R0 ) ->
	{ QS, R1 } = cowboy_req:qs( R0 ),
	{ qs, QS, R1 }.

req_get_headers( R0 ) ->
	{ Headers, R1 } = cowboy_req:headers( R0 ),
	{ headers, Headers, R1 }.

req_get_body( R0 ) ->
	{ ok, Body, R1 } = cowboy_req:body( R0 ),
	{ body, Body, R1 }.

req_get_bindings( R0 ) ->
	{ Bindings, R1 } = cowboy_req:bindings( R0 ),
	{ bindings, Bindings, R1 }.

req_get_path_info( R0 ) ->
	{ PathInfo, R1 } = cowboy_req:path_info( R0 ),
	{ path_info, PathInfo, R1 }.

next_req_id( State = #s{ next_req_id = ReqID } ) ->
	{ ReqID, State #s{ next_req_id = ReqID + 1 } }.


start_link_worker( WorkerSup, ReqID, ReqProps, Handler, HandlerOpts ) ->
	{ok, _} = supervisor:start_child( WorkerSup, [ ReqID, ReqProps, Handler, HandlerOpts ] ).

% log(Report) ->
% 	ok = error_logger:info_report( Report ).
