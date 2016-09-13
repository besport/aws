(* SQS API *)
(* william@corefarm.com *)

let (@:) x xs = x :: xs
let (@?) x xs = match x with
  | (y, None) -> xs
  | (y, Some z) -> (y,z) :: xs

module Opt = struct
  let map f = function
    | None -> None
    | Some x -> Some (f x)
end


module Make = functor (HC : Aws_sigs.HTTP_CLIENT) -> struct

  module X = My_xml
  open Lwt
  open Creds
  module Util = Aws_util


  exception Error of string

  let sprint = Printf.sprintf
  let print = Printf.printf

(* copy/paste from EC2; barko you want to move to Util? *)

  let signed_request
      ~credentials
      ~region
      ?(http_method=`POST)
      ?(http_uri="/")
      ?expires_minutes
      params =

    let http_host = sprint "sqs.%s.amazonaws.com" region in

    let params =
      ("Version", "2009-02-01" ) ::
      ("SignatureVersion", "2") ::
      ("SignatureMethod", "HmacSHA1") ::
      ("AWSAccessKeyId", credentials.aws_access_key_id) ::
      params
    in

    let params = match expires_minutes with
      | Some i -> ("Expires", Util.minutes_from_now i) :: params
      | None -> ("Timestamp", Util.now_as_string ()) :: params
    in

    let signature =
      let sorted_params = Util.sort_assoc_list params in
      let key_equals_value = Util.encode_key_equals_value sorted_params in
      let uri_query_component = String.concat "&" key_equals_value in
      let string_to_sign = String.concat "\n" [
        Util.string_of_t http_method ;
        String.lowercase_ascii http_host ;
        http_uri ;
        uri_query_component
      ]
      in
      let hmac_sha1_encoder = Cryptokit.MAC.hmac_sha1 credentials.aws_secret_access_key in
      let signed_string = Cryptokit.hash_string hmac_sha1_encoder string_to_sign in
      Util.base64 signed_string
    in

    let params = ("Signature", signature) :: params in
    let url = sprint "https://%s%s" http_host http_uri in
    try_lwt HC.post ~body:(`String (Util.encode_post_url params)) url
    with HC.Http_error (code, headers, body) ->
      let msg = try match X.xml_of_string body with
        | X.E ("Response",_,(X.E ("Errors",_,[X.E ("Error",_,[
          X.E ("Code",_,[X.P code]);
          X.E ("Message",_,[X.P message])])]))::_) -> message
        | _ -> body
        with X.ParseError -> body
      in raise (Error (sprint "HTTP %d: %s" code msg))

  type message = {
    message_id : string ;
    receipt_handle : string ;
    body : string
  }

  let create_queue ~credentials ~region ?visibility_timeout name =
    lwt header, body = signed_request ~credentials ~region ~http_uri:("/")
      @@ ("Action", "CreateQueue")
      @: ("QueueName", name)
      @: ("VisibilityTimeout", Opt.map string_of_int visibility_timeout)
      @? [] in
    let xml = X.xml_of_string body in
    let create_queue_response_of_xml = function
      | X.E("CreateQueueResponse", _, kids) -> (
        match kids with
          | [_ ; X.E ("QueueUrl",_, [ X.P url ])] -> url
          | _ -> raise (Error "CreateQueueResponse.queueurl")
      )
      | _ -> raise (Error "CreateQueueResponse")
    in
    return (create_queue_response_of_xml xml)

  let list_queues ~credentials ~region ?prefix () =
    lwt header, body = signed_request ~credentials ~region ~http_uri:("/")
      @@ ("Action", "ListQueues")
      @: ("QueueNamePrefix", prefix)
      @? [] in

    let xml = X.xml_of_string body in
    let queue_url_of_xml = function
      | X.E ("QueueUrl",_ , [ X.P url ]) -> url
      | _ -> raise (Error ("QueueUrlResponse"))
    in
    let list_queues_response_of_xml = function
      | X.E("ListQueuesResponse", _, [
        X.E("ListQueuesResult",_,items) ;
        _ ;
      ]) -> List.map queue_url_of_xml items
      | _ -> raise (Error "ListQueuesRequestsResponse")
    in
    return (list_queues_response_of_xml xml)

  let receive_message ~credentials ~region
      ?(all_attributes = false) ?max_number_of_messages
      ?visibility_timeout ?wait_time_seconds
      ?(encoded=true)
      queue_url =
    lwt header, body = signed_request ~credentials ~region ~http_uri:queue_url
      @@ ("Action", "ReceiveMessage")
      @: ("AttributeName", if all_attributes then Some "All" else None)
      @? ("MaxNumberOfMessages", Opt.map string_of_int max_number_of_messages)
      @? ("VisibilityTimeout", Opt.map string_of_int visibility_timeout)
      @? ("WaitTimeSeconds", Opt.map string_of_int wait_time_seconds)
      @? [] in
    let xml = X.xml_of_string body in
    let fail () = raise
      (Error ("ReceiveMessageResult.message: " ^ X.string_of_xml xml)) in
    let message_of_xml encoded xml =
      let message_id = ref None in
      let receipt_handle = ref None in
      let body = ref None in
      let process_tag = function
        | X.E ("MessageId", _, [ X.P m ]) -> message_id := Some m
        | X.E ("ReceiptHandle", _, [ X.P r ]) -> receipt_handle := Some r
        | X.E ("Body", _, [ X.P b ]) -> body :=
                               Some (if encoded then Util.base64_decoder b else b)
        | X.E ("MD5OfBody", _ , _) -> () (*TODO: actually check?*)
        | _ -> ()
      in
      let (>>=) x f = match x with | Some x -> f x | None -> fail () in
      match xml with
      | X.E ("Message", _, tags) ->
          List.iter process_tag tags;
          !message_id >>= fun message_id ->
          !receipt_handle >>= fun receipt_handle ->
          !body >>= fun body ->
          { message_id ; receipt_handle ; body }
      | _ -> fail ()
    in
    let receive_message_response_of_xml ~encoded = function
      | X.E ("ReceiveMessageResponse",
             _,
             [
               X.E("ReceiveMessageResult",_ , items) ;
               _ ;
             ]) -> List.map (message_of_xml encoded) items
      | _ -> fail ()
    in
    return (receive_message_response_of_xml ~encoded xml)

  let delete_message ~credentials ~region queue_url receipt_handle =
    lwt _ = signed_request ~credentials ~region ~http_uri:queue_url
        [ "Action", "DeleteMessage" ; "ReceiptHandle", receipt_handle ] in
    return ()

  let send_message ~credentials ~region queue_url ?(encoded=true) body =
    lwt header, body = signed_request ~credentials ~region ~http_uri:queue_url
        [ "Action", "SendMessage" ;
          "MessageBody", (if encoded then Util.base64 body else body) ] in
    let xml = X.xml_of_string body in
    let send_message_response_of_xml xml =
      let fail () = raise (Error ("SendMessageResponse: " ^ X.string_of_xml xml)) in
      let (>>=) x f = match x with | Some x -> f x | None -> fail () in
      let message_id = ref None in
      let process_tag = function
        | X.E ("MessageId", _, [ X.P mid ]) -> message_id := Some mid
        | X.E ("MD5OfMessageBody", _, _) -> (); (*TODO: actually check?*)
        | _ -> ()
      in
      let process_tag = function
        | X.E ("SendMessageResult", _, tags) -> List.iter process_tag tags;
        | _ -> ()
      in
      match xml with
        | X.E ("SendMessageResponse", _, tags) ->
            List.iter process_tag tags;
            !message_id >>= fun message_id -> message_id
        | _ -> fail ()
    in
    return (send_message_response_of_xml xml)
end
