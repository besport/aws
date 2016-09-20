module Make = functor (HC : Aws_sigs.HTTP_CLIENT) ->
  struct

module X = My_xml
module Util = Aws_util

type send_data_points = {
  bounce: int ;
  complaints: int ;
  delivery_attempts: int ;
  rejects: int ;
  timestamp: float ;
}

type destination = {
  bcc_addresses: string list ;
  cc_addresses: string list ;
  to_addresses: string list;
}

type content = {
  charset: string;
  data: string;
}

type body = {
  html: content;
  text: content;
}

type message = {
  subject: content;
  body: body;
}

exception Ses_error of string * string * string

let check_error xml =
  try
    let e = X.nodes_of_string "ErrorResponse.Error" xml in
    let type_ = X.data_of_string "Type" e in
    let code = X.data_of_string "Code" e in
    let message = X.data_of_string "Message" e in
    raise (Ses_error (type_,code,message))
  with Not_found -> ()

let endpoint = "https://email.us-east-1.amazonaws.com/"

let build_ses_header ~creds =
  Aws_util.reset_calendar_lang ();
  let date = CalendarLib.Printer.Calendar.sprint "%a, %d %b %Y %T %z" (CalendarLib.Calendar.now ()) in
  let hmac_sha1_encoder = (Cryptokit.MAC.hmac_sha1 creds.Creds.aws_secret_access_key) in
  let sign = Cryptokit.hash_string hmac_sha1_encoder date in
  let sign_64 = Netencoding.Base64.encode sign in
  let h = Printf.sprintf "AWS3-HTTPS AWSAccessKeyId=%s, Algorithm=HmacSHA1, Signature=%s" creds.Creds.aws_access_key_id sign_64 in
  [
    ("Date", date) ;
    ("X-Amzn-Authorization", h)
  ]

let ses_timestamp () =
  CalendarLib.Printer.Calendar.sprint "%FT%T.000Z" (CalendarLib.Calendar.now ())

let make_request ~creds post_params =
  let headers = build_ses_header ~creds in
  let post_params = [
    ("AWSAccessKeyId",creds.Creds.aws_access_key_id);
    ("Timestamp", ses_timestamp ())
  ] @ post_params in
  let body = `String (Util.encode_post_url post_params) in
  lwt _,s = HC.post ~headers ~body endpoint in
  let xml = X.xml_of_string s in
  check_error xml ;
  Lwt.return xml

(****************  SES METHODE ****************)

let delete_verified_email_address ~creds email =
  lwt _ = make_request ~creds [
    ("Action", "DeleteVerifiedEmailAddress");
    ("EmailAddress", email);
  ] in
  Lwt.return ()

let get_send_quota ~creds =
  lwt xml = make_request ~creds [
    ("Action", "GetSendQuota");
  ] in

  let t = X.nodes_of_string "GetSendQuotaResponse.GetSendQuotaResult" xml in
  let max_24_hour_send = float_of_string (X.data_of_string "Max24HourSend" t) in
  let max_send_rate = float_of_string (X.data_of_string "MaxSendRate" t) in
  let sent_last_24_hours = float_of_string (X.data_of_string "SentLast24Hours" t) in

  Lwt.return (max_24_hour_send,max_send_rate,sent_last_24_hours)

let get_send_statistics ~creds =
  lwt xml = make_request ~creds [
    ("Action", "GetSendStatistics");
  ] in
  let t = X.nodes_of_string "GetSendStatisticsResponse.GetSendStatisticsResult.SendDataPoints" xml in
  let l =
    List.fold_left (
      fun datas -> function
        | X.E ("member", _, xml) ->
          let timestamp = X.data_of_string "Timestamp" xml in
          let timestamp = CalendarLib.Printer.Calendar.from_fstring "%FT%TZ" timestamp in
          {
            bounce = int_of_string (X.data_of_string "Bounces" xml) ;
            complaints = int_of_string (X.data_of_string "Complaints" xml) ;
            delivery_attempts = int_of_string (X.data_of_string "DeliveryAttempts" xml) ;
            rejects = int_of_string (X.data_of_string "Rejects" xml) ;
            timestamp = CalendarLib.Calendar.to_unixfloat timestamp ;
          }::datas
        | _ -> datas
    ) [] t
  in
  Lwt.return l

let list_verified_email_addresses ~creds =
  lwt xml = make_request ~creds [
    ("Action", "ListVerifiedEmailAddresses");
  ] in

  let t = X.nodes_of_string "ListVerifiedEmailAddressesResponse.ListVerifiedEmailAddressesResult.VerifiedEmailAddresses" xml in
  let l =
    List.fold_left (
      fun acc -> function
        | X.E ("member", _, [ X.P s ]) -> s::acc
        | _ -> acc
    ) [] t
  in
  Lwt.return l

let build_member dest dest_type acc =
  let l,_ =
    List.fold_left (
      fun (acc,nb) email ->
        if email <> "" then begin
          let k = Printf.sprintf "%s.member.%d" dest_type nb in
          let acc = (k,email)::acc in
          acc, nb+1
        end else (acc,nb)
    ) (acc,1) dest
  in l

(* sending simple emails ******************************************************)

let send_email
    ~creds
    ?reply_to_addresses
    ?return_path
    ~destination
    ~source
    ~message () =

  let build_members dests acc =
    let acc = build_member dests.to_addresses "Destination.ToAddresses" acc in
    let acc = build_member dests.bcc_addresses "Destination.BccAddresses" acc in
    let acc = build_member dests.cc_addresses "Destination.CcAddresses" acc in
    match reply_to_addresses with
      | Some r -> build_member r "ReplyToAddresses" acc
      | None -> acc
  in

  let build_message message acc =
    List.fold_left
      (fun acc ((k,v) as el) ->
        if v = "" then acc
        else el::acc)
      acc
      [
        "Message.Subject.Data", message.subject.data;
        "Message.Subject.Charset", message.subject.charset;
        "Message.Body.Text.Data", message.body.text.data;
        "Message.Body.Text.Charset", message.body.text.charset;
        "Message.Body.Html.Data", message.body.html.data;
        "Message.Body.Html.Charset", message.body.html.charset;
      ]
  in

  let params =
    build_members destination [
      ("Action", "SendEmail") ;
      ("Source", source) ;
    ]
  in
  let params = build_message message params in
  let params =
    match return_path with
      | Some rp -> ("ReturnPath", rp)::params
      | None -> params
  in

  lwt xml = make_request ~creds params in
  Lwt.return
    (X.data_of_string "SendEmailResponse.SendEmailResult.MessageId" [ xml ])

(* sending raw emails ***********************************************************)

let send_raw_email ~creds ?destinations ?source ~raw_message () =
  let build_members dests acc =
    match dests with
    | Some d -> build_member d "Destinations" acc
    | None -> acc
  in

  let params =
    match source with
    | Some s -> [ ("Source", s) ; ("Action", "SendRawEmail") ];
    | None -> [ ("Action", "SendRawEmail") ];
  in

  let params = build_members destinations params in
  let raw_message = Netencoding.Base64.encode raw_message in
  let params = ("RawMessage.Data", raw_message) :: params in
  lwt xml = make_request ~creds params in
  Lwt.return (X.data_of_string "SendRawEmailResponse.SendRawEmailResult.MessageId" [xml])

let verify_email_address ~creds email =
  lwt xml =
    make_request ~creds [
      ("Action", "VerifyEmailIdentity");
      ("EmailAddress", email);
    ] in
  Lwt.return (X.data_of_string "VerifyEmailIdentityResponse.ResponseMetadata.RequestId" [xml])

(* some helpers ***************************************************************)

let send_basic_email
    ~creds
    ?message_text
    ?bcc
    ?cc
    ?charset
    ?reply_to_addresses
    ?return_path
    ~from_
    ~to_
    ~subject
    ?message_html () =
  let destination = {
    to_addresses = to_ ;
    bcc_addresses = (match bcc with | Some bcc -> bcc | None -> []) ;
    cc_addresses = (match cc with | Some cc -> cc | None -> [] );
  } in

  let charset = match charset with | Some c -> c | None -> "" in

  let message = {
    subject = { charset; data = subject } ;
    body = {
      html = { charset = (match message_html with Some "" | None -> "" | _ -> charset);
               data = (match message_html with | Some m -> m | None -> "") } ;
      text = { charset = (match message_text with Some "" | None -> "" | _ -> charset);
               data = (match message_text with | Some m -> m | None -> "") }
    }
  } in

  send_email ~creds ?reply_to_addresses ?return_path ~destination ~source:from_ ~message ()

let get_raw_send_statistics ~creds =
  lwt stats = get_send_statistics ~creds in
  Lwt.return (
    List.map (fun s ->
      (s.bounce,s.complaints,s.delivery_attempts,s.rejects,s.timestamp)
    ) stats
  )

end
