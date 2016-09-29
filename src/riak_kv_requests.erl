%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


-module(riak_kv_requests).

%% API
-export([request_type/1,
         request_hash/1]).

-export([new_put_request/5,
         is_coordinated_put/1,
         get_bucket_key/1,
         get_object/1,
         set_object/2,
         get_request_id/1,
         get_start_time/1,
         get_options/1,
         remove_option/2
    ]).



-type bucket_key() :: {binary(),binary()}.
-type object() :: term().
-type request_id() :: non_neg_integer().
-type start_time() :: non_neg_integer().
-type request_options() :: [any()].

-record(riak_kv_put_req_v1,
        { bkey :: bucket_key(),
          object :: object(),
          req_id :: request_id(),
          start_time :: start_time(),
          options :: request_options()}).

-type request() :: #riak_kv_put_req_v1{}.

-type request_type() :: 'kv_put_request' | 'unknown'.

-export_type([request/0,
              request_type/0]).

-spec request_type(request()) -> request_type().
request_type(#riak_kv_put_req_v1{}) -> kv_put_request;
request_type(_) -> unknown.

request_hash(#riak_kv_put_req_v1{bkey=BKey}) ->
    riak_core_util:chash_key(BKey);
request_hash(_) ->
    undefined.


-spec new_put_request(bucket_key(),
                      object(),
                      request_id(),
                      start_time(),
                      request_options()) -> request().
new_put_request(BKey, Object, ReqId, StartTime, Options) ->
    #riak_kv_put_req_v1{bkey = BKey,
                        object = Object,
                        req_id = ReqId,
                        start_time = StartTime,
                        options = Options}.

-spec is_coordinated_put(#riak_kv_put_req_v1{}) -> boolean().
is_coordinated_put(#riak_kv_put_req_v1{options=Options}) ->
    proplists:get_value(coord, Options, false).

get_bucket_key(#riak_kv_put_req_v1{bkey=BKey}) ->
    BKey.

get_object(#riak_kv_put_req_v1{object = Object}) ->
    Object.

get_request_id(#riak_kv_put_req_v1{req_id = ReqId}) ->
    ReqId.

get_start_time(#riak_kv_put_req_v1{start_time = StartTime}) ->
    StartTime.

get_options(#riak_kv_put_req_v1{options = Options}) ->
    Options.

set_object(#riak_kv_put_req_v1{}=Req, Object) ->
    Req#riak_kv_put_req_v1{object = Object}.

remove_option(#riak_kv_put_req_v1{options = Options}=Req, Option) ->
    NewOptions = proplists:delete(Option, Options),
    Req#riak_kv_put_req_v1{options = NewOptions}.





