(* SQS API *)
(* william@corefarm.com *)

module Make = functor (HC : Aws_sigs.HTTP_CLIENT) ->
struct
  module X = My_xml



  open Lwt
  open Creds

  module Util = Aws_util


  exception Error of string

  let sprint = Printf.sprintf
  let print = Printf.printf

(* copy/paste from EC2; barko you want to move to Util? *)


let signed_request
    ?region
    ?(http_method=`POST)
    ?(http_uri="/")
    ?expires_minutes
    creds
    params =

  let http_host =
    match region with
      | Some r -> sprint "sqs.%s.amazonaws.com" r
      | None -> "sqs.us-east-1.amazonaws.com"
  in

  let params =
    ("Version", "2009-02-01" ) ::
      ("SignatureVersion", "2") ::
      ("SignatureMethod", "HmacSHA1") ::
      ("AWSAccessKeyId", creds.aws_access_key_id) ::
        params
  in

  let params =
    match expires_minutes with
      | Some i -> ("Expires", Util.minutes_from_now i) :: params
      | None -> ("Timestamp", Util.now_as_string ()) :: params
  in

  let signature =
    let sorted_params = Util.sort_assoc_list params in
    let key_equals_value = Util.encode_key_equals_value sorted_params in
    let uri_query_component = String.concat "&" key_equals_value in
    let string_to_sign = String.concat "\n" [
      Util.string_of_t http_method ;
      String.lowercase http_host ;
      http_uri ;
      uri_query_component
    ]
    in
    let hmac_sha1_encoder = Cryptokit.MAC.hmac_sha1 creds.aws_secret_access_key in
    let signed_string = Cryptokit.hash_string hmac_sha1_encoder string_to_sign in
    Util.base64 signed_string
  in

  let params = ("Signature", signature) :: params in
  sprint "https://%s%s" http_host http_uri, params


  let error_msg body =
    match X.xml_of_string body with
      | X.E ("Response",_,(X.E ("Errors",_,[X.E ("Error",_,[
        X.E ("Code",_,[X.P code]);
        X.E ("Message",_,[X.P message])
      ])]))::_) ->
        `Error message
      | _ -> `Error "unknown message"



(* xml handling utilities *)
  let queue_url_of_xml = function
    | X.E ("QueueUrl",_ , [ X.P url ]) -> url
    | _ ->
      raise (Error ("QueueUrlResponse"))

  let list_queues_response_of_xml = function
    | X.E("ListQueuesResponse", _, [
      X.E("ListQueuesResult",_,items) ;
      _ ;
    ]) ->
      List.map queue_url_of_xml items
    | _ ->
      raise (Error "ListQueuesRequestsResponse")

  let create_queue_response_of_xml = function
    | X.E("CreateQueueResponse", _, kids) -> (
      match kids with
        | [_ ; X.E ("QueueUrl",_, [ X.P url ])] -> (
          url
        )
        | _ -> raise (Error "CreateQueueResponse.queueurl")
    )
    | _ -> raise (Error "CreateQueueResponse")

  type message =
      {
        message_id : string ;
        receipt_handle : string ;
        body : string }

  let message_of_xml encoded = function
    | X.E ("Message",
           _,
           X.E ("MessageId", _, [ X.P message_id ])
           :: X.E ("ReceiptHandle", _, [ X.P receipt_handle ])
           :: X.E ("MD5OfBody", _ , _)
           :: X.E ("Body", _, [ X.P body ]) :: attributes
    ) -> { message_id ; receipt_handle ; body = (if encoded then Util.base64_decoder body else body) }

    | _ -> raise (Error "ReceiveMessageResult.message")

  let receive_message_response_of_xml ~encoded = function
    | X.E ("ReceiveMessageResponse",
           _,
           [
             X.E("ReceiveMessageResult",_ , items) ;
             _ ;
           ]) -> List.map (message_of_xml encoded) items

    | _ -> raise (Error "ReceiveMessageResponse")

  let send_message_response_of_xml = function
    | X.E ("SendMessageResponse",
           _,
           [
             X.E("SendMessageResult",_ ,
                 [
                   X.E ("MD5OfMessageBody", _, _) ;
                   X.E ("MessageId", _, [ X.P message_id ])
                 ]) ;
             _ ;
           ]) -> message_id

    | _ -> raise (Error "SendMessageResponse")


(* create queue *)
  let create_queue ?(default_visibility_timeout=30) creds queue_name =

    let url, params = signed_request ~http_uri:("/") creds
      [
        "Action", "CreateQueue" ;
        "QueueName", queue_name ;
        "DefaultVisibilityTimeout", string_of_int default_visibility_timeout ;
      ] in
    try_lwt
  let ps = Util.encode_post_url params in
  print "posting request on %s: %s\n" url ps ;
  lwt header, body = HC.post ~body:(`String ps) url in


  let xml = X.xml_of_string body in
  return (`Ok (create_queue_response_of_xml xml))
  with HC.Http_error (code, _, body) -> print "Error %d %s\n" code body ; return (error_msg body)

(* list existing queues *)
  let list_queues ?prefix creds =

    let url, params = signed_request ~http_uri:("/") creds
      (("Action", "ListQueues")
       :: (match prefix with
           None -> []
         | Some prefix -> [ "QueueNamePrefix", prefix ])) in
    try_lwt
  let ps = Util.encode_post_url params in
  lwt header, body = HC.post ~body:(`String ps) url in

  let xml = X.xml_of_string body in
  return (`Ok (list_queues_response_of_xml xml))
    with HC.Http_error (code, _, body) -> print "Error %d %s\n" code body ; return (error_msg body)

(* get messages from a queue *)
  let receive_message ?(attribute_name="All") ?(max_number_of_messages=1) ?(visibility_timeout=30) ?(encoded=true) creds queue_url =
    let url, params = signed_request creds ~http_uri:queue_url
      [
        "Action", "ReceiveMessage" ;
        "AttributeName", attribute_name ;
        "MaxNumberOfMessages", string_of_int max_number_of_messages ;
        "VisibilityTimeout", string_of_int visibility_timeout ;
      ] in
    try_lwt
  lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in

  let xml = X.xml_of_string body in
  return (`Ok (receive_message_response_of_xml ~encoded xml))
  with HC.Http_error (_, _, body) -> return (error_msg body)

(* delete a message from a queue *)

  let delete_message creds queue_url receipt_handle =
    let url, params = signed_request creds ~http_uri:queue_url
      [
        "Action", "DeleteMessage" ;
        "ReceiptHandle", receipt_handle
      ] in
    try_lwt
  lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in
  ignore (header) ;
  ignore (body);
  return (`Ok ())
    with HC.Http_error (_, _, body) -> return (error_msg body)

(* send a message to a queue *)

  let send_message creds queue_url ?(encoded=true) body =
    let url, params = signed_request creds ~http_uri:queue_url
      [
        "Action", "SendMessage" ;
        "MessageBody", (if encoded then Util.base64 body else body)
      ] in
    try_lwt
  lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in

  let xml = X.xml_of_string body in
  return (`Ok (send_message_response_of_xml xml))
  with HC.Http_error (_, _, body) ->  return (error_msg body)
end
