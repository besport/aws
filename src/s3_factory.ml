module Make = functor (HC : Aws_sigs.HTTP_CLIENT) ->
  struct

module C = CalendarLib.Calendar
module P = CalendarLib.Printer.CalendarPrinter
module K = Cryptokit

module X = My_xml

open Lwt

module Util = Aws_util

let sprintf = Printf.sprintf

type region = [
| `US_EAST_1
| `US_WEST_1
| `US_WEST_2
| `EU_WEST_1
| `AP_SOUTHEAST_1
| `AP_NORTHEAST_1 ]

let string_of_region = function
  | `US_EAST_1      -> "us-east-1"
  | `US_WEST_1      -> "us-west-1"
  | `US_WEST_2      -> "us-west-2"
  | `EU_WEST_1      -> "eu-west-1"
  | `AP_SOUTHEAST_1 -> "ap-southeast-1"
  | `AP_NORTHEAST_1 -> "ap-northeast-1"

let region_of_string = function
  | "us-east-1"        -> `US_EAST_1
  | "us-west-1"        -> `US_WEST_1
  | "us-west-2"        -> `US_WEST_2
  | "eu-west-1"        -> `EU_WEST_1
  | "ap-southeast-1"   -> `AP_SOUTHEAST_1
  | "ap-northeast-1"   -> `AP_NORTHEAST_1
  | x                  -> raise (Invalid_argument ("string_of_region: " ^ x))

let service_url_of_region = function
  | `US_EAST_1      -> "http://s3.amazonaws.com/"
  | `US_WEST_1      -> "http://s3-us-west-1.amazonaws.com/"
  | `US_WEST_2      -> "http://s3-us-west-2.amazonaws.com/"
  | `EU_WEST_1      -> "http://s3-eu-west-1.amazonaws.com/"
  | `AP_SOUTHEAST_1 -> "http://s3-ap-southeast-1.amazonaws.com/"
  | `AP_NORTHEAST_1 -> "http://s3-ap-northeast-1.amazonaws.com/"

let create_bucket_configuration_xml s =
  sprintf
    "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
       <LocationConstraint>%s</LocationConstraint>
     </CreateBucketConfiguration>"
    s

(* wow this is bad *)
let location_constraint_xml_of_region = function
  | `US_EAST_1      -> ""
  | `US_WEST_1      -> create_bucket_configuration_xml "us-west-1"
  | `US_WEST_2      -> create_bucket_configuration_xml "us-west-2"
  | `EU_WEST_1      -> create_bucket_configuration_xml "EU"
  | `AP_SOUTHEAST_1 -> create_bucket_configuration_xml "ap-southeast-1"
  | `AP_NORTHEAST_1 -> create_bucket_configuration_xml "ap-northeast-1"

let now_as_string () =
  Aws_util.reset_calendar_lang ();
  P.sprint "%a, %d %b %Y %H:%M:%S GMT" (C.now ())

type amz_acl = [
| `Private (* not using [`private] because [private] is an ocaml keyword *)
| `public_read
| `public_read_write
| `authenticated_read
| `bucket_owner_read
| `bucket_owner_full_control
]


let string_of_amz_acl = function
  | `Private                   -> "private"
  | `public_read               -> "public-read"
  | `public_read_write         -> "public-read-write"
  | `authenticated_read        -> "authenticated-read"
  | `bucket_owner_read         -> "bucket-owner-read"
  | `bucket_owner_full_control -> "bucket-owner-full-control"

let sign key string_to_sign =
  let hmac_sha1 = K.MAC.hmac_sha1 key in
  let hashed_string_to_sign = K.hash_string hmac_sha1 string_to_sign in
  Util.base64 hashed_string_to_sign

exception Error of string

module StringMap = Map.Make(String)

let group_by_key kv_list =
  let map = List.fold_left (
    fun map (k,v) ->
      let v_list =
        try
          StringMap.find k map
        with Not_found ->
          []
      in
      StringMap.add k (v :: v_list) map
  ) StringMap.empty kv_list
  in
  StringMap.fold (
    fun k v_list accu ->
      (k, v_list) :: accu
  ) map []

let find_element kids key =
  try
    Some (
      List.find (
        fun kid ->
          match kid with
            | X.E ( k, _, _ ) when k = key -> true
            | _ -> false
      ) kids
    )
  with Not_found ->
    None

(* get the data of an X.P child node of a node in [kids] whose tag is
   [key] *)
let find_p_child_fail kids key =
  match find_element kids key with
    | None -> raise (Error ("\"" ^ key ^ "\" not found"))
    | Some (X.E( _, _, [X.P data])) -> data
    | Some _ -> raise (Error ("\"" ^ key ^ "\" doesn't have data child"))

(* replace a substring of whitespace with a single space *)
let squash_whitespace =
  Pcre.replace ~rex:(Pcre.regexp "[[:space:]]+") ~templ:" "

type sub_resource = [
| `acl
| `location
| `logging
| `notification
| `part_number
| `policy
| `request_payment
| `torrent
| `upload_id
| `uploads
| `version_id
| `versioning
| `versions
]

let sub_resource_of_string = function
| "acl"            -> `acl
| "location"       -> `location
| "logging"        -> `logging
| "notification"   -> `notification
| "partNumber"     -> `part_number
| "policy"         -> `policy
| "requestPayment" -> `request_payment
| "torrent"        -> `torrent
| "uploadId"       -> `upload_id
| "uploads"        -> `uploads
| "versionId"      -> `version_id
| "versioning"     -> `versioning
| "versions"       -> `versions
| _ -> raise (Error "subresource")

let string_of_sub_resource = function
| `acl              -> "acl"
| `location         -> "location"
| `logging          -> "logging"
| `notification     -> "notification"
| `part_number      -> "partNumber"
| `policy           -> "policy"
| `request_payment  -> "requestPayment"
| `torrent          -> "torrent"
| `upload_id        -> "uploadId"
| `uploads          -> "uploads"
| `version_id       -> "versionId"
| `versioning       -> "versioning"
| `versions         -> "versions"

class buffer size =
  let b = Buffer.create size in
object
  method add s = Buffer.add_string b s
  method contents = Buffer.contents b
end

let auth_hdr
    ?(http_method=`GET)
    ?(content_type="")
    ?(content_md5="")
    ?(date="")
    ?(bucket="")
    ?(request_uri="")
    ?(amz_headers=[])
    ?(sub_resources=[])
    creds =

  let canonicalized_amz_headers =
    match amz_headers with
      | [] -> ""
      | _ -> (
        let downcased_keys = List.map (
          fun (k,v) -> String.lowercase k, squash_whitespace v
        ) amz_headers
        in
        let grouped = group_by_key downcased_keys in
        let merged_values = List.map (
          fun (k,v_list) -> k, (String.concat "," v_list)
        ) grouped
        in
        let sorted_by_key = Util.sort_assoc_list merged_values in

        let buf = new buffer 10 in
        List.iter (
          fun (k,v) ->
            buf#add k; buf#add ":"; buf#add v; buf#add "\n"
        ) sorted_by_key;
        buf#contents
      )
  in

  let canonicalized_sub_resources =
    match sub_resources with
      | [] -> ""
      | _ -> (
        let sub_resources_s = List.map (
          fun (k,vo) -> string_of_sub_resource k, vo
        ) sub_resources
        in
        let sorted_sub_resources_s = Util.sort_assoc_list sub_resources_s in

        let buf = new buffer 10 in
        buf#add "?";

        List.iter (
          fun (k, v_opt) ->
            match v_opt with
              | Some v -> buf#add k; buf#add "&"; buf#add v
              | None -> buf#add k
        ) sorted_sub_resources_s;
        buf#contents
      )
  in
  let canonicalized_resource =
    "/" ^ bucket ^ request_uri ^ canonicalized_sub_resources
  in

  let string_to_sign =
    let buf = new buffer 100 in
    buf#add (Util.string_of_t http_method); buf#add "\n";
    buf#add content_md5; buf#add "\n";
    buf#add content_type; buf#add "\n";
    buf#add date; buf#add "\n";
    buf#add canonicalized_amz_headers;
    buf#add canonicalized_resource;
    buf#contents
  in
  let signature = sign creds.Creds.aws_secret_access_key string_to_sign in
  "Authorization", sprintf "AWS %s:%s" creds.Creds.aws_access_key_id signature


let error_msg body =

  (* <Error><Code>SomeMessage</Code>...</Error> *)
  try
    match X.xml_of_string body with
      | X.E ("Error",_, kids ) -> (
        match find_element kids "Code" with
          | Some (X.E( "Code", _, [X.P msg] )) -> return (`Error msg)
          | _ -> fail (Error body)
      )

      | _ ->
        (* complain if can't interpret the xml, and then just use the
         entire body as the payload for the exception *)
        fail (Error body)

  with Xmlm.Error (_,err) ->
    fail (Error body)


let s3_region_regexp = Pcre.regexp "s3(-(.*))?"

let region_of_endpoint s =
  match Pcre.split ~pat:"\\." s with
    | [ _bucket ; s3_region_s ; "amazonaws" ; "com" ] -> (
      (match Pcre.extract ~rex:s3_region_regexp s3_region_s with
        | [| _; _; region_s |] ->  (
          try
            Some (region_of_string region_s)
          with Invalid_argument _ ->
            None
        )

        | _ -> None

      )
    )
    | _ -> None


(* problem/bug?: if the specified region is us-east-1, and this not
   the correct region for that bucket, the redirected endpoint is
   <bucket>.s3.amazonaws.com, which does not reveal the correct region
   of the bucket. *)
let permanent_redirect_of_string body =
  (* <Endpoint>mybucket.us-west-1.s3.amazonaws.com</Endpoint>, or
     <Endpoint>mybucket.s3.amazonaws.com</Endpoint>
  *)
  try
    match X.xml_of_string body with
      | X.E ("Error",_, kids ) -> (

        match
          find_element kids "Code",
          find_element kids "Endpoint"
        with
          | Some (X.E( "Code", _, [X.P "PermanentRedirect"] )),
            Some (X.E( "Endpoint", _, [X.P endpoint] )) ->
            return (`PermanentRedirect (region_of_endpoint endpoint))

          | Some (X.E( "Code", _, [X.P msg] )), _ -> return (`Error msg)
          | _ -> fail (Error body)

      )

      | _ ->
        (* complain if can't interpret the xml, and then just use the
         entire body as the payload for the exception *)
        fail (Error body)

  with Xmlm.Error (_,err) ->
    fail (Error body)

let get_object_h creds_opt region ~bucket ~objekt  =
  let date = now_as_string () in
  let authorization_header =
    match creds_opt with
      | None -> [] (* anonymous *)
      | Some creds ->
        [ auth_hdr
          ~http_method:`GET
          ~date
          ~bucket
          ~request_uri:("/" ^ objekt)
          creds
        ]
  in

  let headers = ("Date", date) :: authorization_header in
  let request_url = sprintf "%s%s/%s" (service_url_of_region region)
    (Util.encode_url bucket) (objekt)
  in
  headers, request_url

(* get object *)
let get_object_s creds_opt region ~bucket ~objekt =
  let headers, request_url = get_object_h creds_opt region ~bucket ~objekt  in

  try_lwt
    lwt _, body = HC.get ~headers request_url in
    return (`Ok body)
  with
    | HC.Http_error (404,_,_) -> return `NotFound
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_, _, body) -> (
        error_msg body >>= function
          | `Error "AccessDenied" -> return `AccessDenied
          | `Error _ as err -> return err
      )

let get_object ?byte_range creds_opt region ~bucket ~objekt ~path =
  let headers, request_url = get_object_h creds_opt region ~bucket ~objekt in
  let byte_range_header =
    match byte_range with
      | None -> []
      | Some (start,fini) -> ["Range", sprintf "bytes=%d-%d" start fini]
  in
  let headers = headers @ byte_range_header in
  let flags = [ Unix.O_CREAT; Unix.O_WRONLY; Unix.O_APPEND; Unix.O_TRUNC ] in
  lwt outchan = Lwt_io.open_file ~flags ~mode:Lwt_io.output path in
  let close_no_err () =
    (* close (a possibly already closed) channel *)
    try_lwt Lwt_io.close outchan with _ -> return ()
  in
  lwt res =
    try_lwt
      lwt _ = HC.get_to_chan ~headers request_url outchan in
      return `Ok
    with
      | HC.Http_error (404,_,_) -> return `NotFound
      | HC.Http_error (301,_,_) ->
        (* error message possibly stored in body, so read it
           back from the file in which it was just stored: *)
        lwt () = close_no_err () in
        lwt body = Util.file_contents path in
        permanent_redirect_of_string body
      | HC.Http_error (_, _,_) ->
        lwt () = close_no_err () in
        lwt body = Util.file_contents path in
        error_msg body >>= function
          | `Error "AccessDenied" -> return `AccessDenied
          | `Error _ as err -> return err
  in
  lwt () = close_no_err () in
  return res

(* create bucket *)
let create_bucket creds region bucket amz_acl =
  let date = now_as_string () in
  let amz_headers = ["x-amz-acl", string_of_amz_acl amz_acl ] in
  let request_url = sprintf "%s%s" (service_url_of_region region) bucket in
  let body = location_constraint_xml_of_region region in

  (* [Http_client] puts a default content type whenever there's a
     non-empty body; since that contenty-type must figure into the
     signature, we have to explicity set it here *)
  let content_type = "application/xml" in

  let authorization_header = auth_hdr
    ~http_method:`PUT ~bucket ~amz_headers ~date ~content_type creds
  in
  let headers = authorization_header ::
    ("Date", date) ::
    ("Content-Type", content_type) ::
    amz_headers in
  try_lwt
    lwt _ = HC.put ~headers ~body:(`String body) request_url in
    return `Ok
  with HC.Http_error (_, _, body) ->
    error_msg body


(* delete bucket *)
let delete_bucket creds region bucket =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`DELETE ~bucket ~date creds
  in
  let service_url = service_url_of_region region in
  let request_url = sprintf "%s%s" service_url (Util.encode_url bucket) in
  let headers = [ authorization_header ; "Date", date ] in
  try_lwt
    lwt _ = HC.delete ~headers request_url in
    (* success signaled via a 204 *)
    fail (Error "delete_bucket")
  with
    | HC.Http_error (204, _, _   ) -> return `Ok
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  , _, body) -> error_msg body

(* list buckets *)
let rec bucket = function
  | X.E (
    "Bucket",_, [
      X.E ("Name",_,        [X.P name           ]) ;
      X.E ("CreationDate",_,[X.P creation_date_s])
    ] )->
    (object
      method name = name
      method creation_date = creation_date_s
     end)
  | _ -> raise (Error "ListAllMyBucketsResult:2")

and list_all_my_buckets_result_of_xml = function
  | X.E ("ListAllMyBucketsResult",_, [_ ; X.E ("Buckets",_,buckets) ] ) ->
    List.map bucket buckets
  | _ -> raise (Error "ListAllMyBucketsResult:1")

let list_buckets creds region =
  let date = now_as_string () in
  let authorization_header = auth_hdr ~http_method:`GET ~date creds in
  let headers = [ authorization_header ; "Date", date ] in
  let request_url = service_url_of_region region in
  try_lwt
    lwt headers, body = HC.get ~headers request_url in
    try
      let buckets = list_all_my_buckets_result_of_xml (X.xml_of_string body) in
      return (`Ok buckets)
    with (Error _) as exn ->
      fail exn
  with
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  , _, body) -> error_msg body


(* put object *)
let noop () = return ()

let put_object
    ?(content_type="binary/octet-stream")
    ?(amz_acl=`Private)
    creds
    region
    ~bucket
    ~objekt
    ~body =
  let date = now_as_string () in
  let amz_headers = [ "x-amz-acl", string_of_amz_acl amz_acl ] in
  let authorization_header = auth_hdr
    ~http_method:`PUT
    ~content_type
    ~amz_headers
    ~date
    ~bucket
    ~request_uri:("/" ^ (Util.encode_url objekt))
    creds
  in
  let bucket_object = (Util.encode_url bucket) ^ "/" ^
    (Util.encode_url objekt)
  in
  let request_url = (service_url_of_region region) ^ bucket_object in
  let headers = ("Date", date) :: ("Content-Type", content_type) ::
    authorization_header :: amz_headers
  in
  lwt request_body, close =
    match body with
      | `String contents ->
        return (`String contents, noop)
      | `File path ->
        let file_size = Util.file_size path in
        let flags = [Unix.O_RDONLY] in
        lwt inchan = Lwt_io.open_file ~flags ~mode:Lwt_io.input path in
        return (`InChannel (file_size, inchan), fun () -> Lwt_io.close inchan)
  in
  try_lwt
    lwt _ = HC.put ~headers ~body:request_body request_url in
    lwt () = close () in
    return `Ok
  with exn ->
    lwt () = close () in
    match exn with
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_, _, body) ->  (
        error_msg body >>= function
          | `Error "AccessDenied" -> return `AccessDenied
          | `Error _ as err -> return err
      )
    | _ -> fail exn


(* get object metadata *)
let assoc_header headers err name =
  let name = String.lowercase name in
  try
    let _, v = List.find (fun (n,v) -> (String.lowercase n) = name) headers in
    v
  with Not_found ->
    raise (Error err)

let get_object_metadata creds region ~bucket ~objekt =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`HEAD ~date ~bucket ~request_uri:("/" ^ objekt) creds
  in
  let headers = [ "Date", date ; authorization_header ] in
  let bucket_object = (Util.encode_url bucket) ^ "/" ^ (Util.encode_url objekt) in
  let request_url = (service_url_of_region region) ^ bucket_object in
  try_lwt
    lwt response_headers, _ = HC.head ~headers request_url in
    let find k = assoc_header response_headers ("GetObjectMetadata:" ^ k) k in
    let content_type = find "Content-Type" in
    let etag = find "ETag" in
    let last_modified_s = find "Last-Modified" in
    let content_length = int_of_string (find "Content-Length") in
    let last_modified = Util.unixfloat_of_amz_date_string last_modified_s in
    let meta = (object
      method content_type = content_type
      method etag = etag
      method last_modified = last_modified
      method content_length = content_length
    end)
    in
    return (`Ok meta)
  with
    | HC.Http_error (404,_, _   ) -> return `NotFound
    | HC.Http_error (301,_, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  ,_, body) -> error_msg body


(* list objects *)
let option_pcdata err = function
  | [X.P x] -> Some x
  | [] -> None
  | _ -> raise (Error err)

let rec list_bucket_result_of_xml = function
  | X.E ("ListBucketResult",_,kids) -> (
      match kids with
        | X.E ("Name",_,[X.P name]) ::
            X.E ("Prefix",_,prefix_opt) ::
            X.E ("Marker",_,marker_opt) ::
            X.E ("MaxKeys",_,[X.P max_keys]) ::
            X.E ("IsTruncated",_,[X.P is_truncated]) ::
            contents ->

            let prefix_opt = option_pcdata "ListBucketResult:prefix" prefix_opt in
            let marker_opt = option_pcdata "ListBucketResult:marker" marker_opt in
            let max_keys = int_of_string max_keys in
            let is_truncated = bool_of_string is_truncated in
            let contents = contents_of_xml contents in

            (object
               method name = name
               method prefix = prefix_opt
               method marker = marker_opt
               method max_keys = max_keys
               method is_truncated = is_truncated
               method objects = contents
             end)
        | _ ->
            raise (Error "ListBucketResult:k")
    )
  | _ ->
      raise (Error "ListBucketResult:t")

and contents_of_xml contents =
  List.map objects_of_xml contents

and objects_of_xml = function
  | X.E ("Contents",_, kids ) ->
      let key = find_p_child_fail kids "Key" in
      let etag = find_p_child_fail kids "ETag" in
      let size = find_p_child_fail kids "Size" in
      let storage_class = find_p_child_fail kids "StorageClass" in
      let last_modified = find_p_child_fail kids "LastModified" in

      let identity =
        match find_element kids "Owner" with
          | Some (
              X.E (
                "Owner",_,[
                  X.E ("ID",_,[X.P owner_id]);
                  X.E ("DisplayName",_,[X.P owner_display_name])
                ]
              )
            ) ->
              Some (
                object
                  method id = owner_id
                  method display_name = owner_display_name
                end
              )
          | Some _ ->
              raise (Error "ListBucket: owner")

          | None -> None
      in
      let last_modified = Util.unixfloat_of_amz_date_string last_modified in
      let size = int_of_string size in
      (object
         method name = key
         method last_modified = last_modified
         method etag = etag
         method size = size
         method storage_class = storage_class
         method owner = identity
       end)
  | _ -> raise (Error "ListBucketResult:c")


let list_objects ?prefix ?marker ?max_keys creds region bucket =
  let date = now_as_string () in
  let authorization_header = auth_hdr ~http_method:`GET ~date ~bucket creds in
  let headers = [ "Date", date ; authorization_header ] in

  let params = [] in
  let params =
    match prefix with
      | None -> params
      | Some p -> ("prefix", p) :: params
  in
  let params =
    match marker with
      | None -> params
      | Some m ->  ("marker", m) :: params
  in
  let params =
    match max_keys with
      | None -> params
      | Some mk -> ("max-keys", string_of_int mk) :: params
  in

  let request_url = (service_url_of_region region) ^ (Util.encode_url bucket) ^
    "?" ^ (Netencoding.Url.mk_url_encoded_parameters params) in

  try_lwt
    lwt response_headers, response_body = HC.get ~headers request_url in
    return (`Ok (list_bucket_result_of_xml (X.xml_of_string response_body)))
  with
    | HC.Http_error (404, _, _   ) -> return `NotFound
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  , _, body) -> error_msg body

(* get bucket acl *)
type permission = [
| `read
| `write
| `read_acp
| `write_acp
| `full_control
]

let string_of_permission = function
  | `read -> "READ"
  | `write -> "WRITE"
  | `read_acp -> "READ_ACP"
  | `write_acp -> "WRITE_ACP"
  | `full_control -> "FULL_CONTROL"

let permission_of_string = function
  | "READ" -> `read
  | "WRITE" -> `write
  | "READ_ACP" -> `read_acp
  | "WRITE_ACP" -> `write_acp
  | "FULL_CONTROL" -> `full_control
  | x -> raise (Error (sprintf "invalid permission %S" x))


class canonical_user ~id ~display_name =
object
  method id : string = id
  method display_name : string = display_name
end

type group = [
| `AllUsers
| `Authenticated
| `LogDelivery
]

let string_of_group = function
  | `AllUsers      -> "http://acs.amazonaws.com/groups/global/AllUsers"
  | `Authenticated -> "http://acs.amazonaws.com/groups/global/Authenticated"
  | `LogDelivery   -> "http://acs.amazonaws.com/groups/s3/LogDelivery"

let group_of_string = function
  | "http://acs.amazonaws.com/groups/global/AllUsers"      -> `AllUsers
  | "http://acs.amazonaws.com/groups/global/Authenticated" -> `Authenticated
  | "http://acs.amazonaws.com/groups/s3/LogDelivery"       -> `LogDelivery
  | x -> raise (Error (sprintf "invalid group %S" x))

type identity = [
| `amazon_customer_by_email of string
| `canonical_user of canonical_user
| `group of group
]

type grant = identity * permission

class acl owner grants =
object
  method owner : identity = owner
  method grants : grant list = grants
end

let string_of_identity = function
| `amazon_customer_by_email em -> "AmazonCustomerByEmail " ^ em
| `canonical_user cn -> sprintf "CanonicalUser (%s,%s)" cn#id cn#display_name
| `group g -> "Group " ^ (string_of_group g)

let tag_of_identity = function
| `amazon_customer_by_email _ -> "AmazonCustomerByEmail"
| `canonical_user _ -> "CanonicalUser"
| `group _ -> "Group"

let identity_of_xml = function
  | [X.E ("ID",_,[X.P id]);
     X.E ("DisplayName",_,[X.P display_name])
    ] ->
    `canonical_user (new canonical_user id display_name)

  | [X.E ("EmailAddress",_,[X.P email_address])] ->
    `amazon_customer_by_email email_address

  | [X.E ("URI",_,[X.P group])] ->
    `group (group_of_string group)

  | _ ->
    raise (Error "grantee")

let grant_of_xml = function
  | X.E ("Grant",_, [
    X.E ("Grantee", grantee_atts, grantee_x);
    X.E ("Permission",_,[X.P permission_s])  ]) ->
    let grantee = identity_of_xml grantee_x in
    let permission = permission_of_string permission_s in
    (grantee, permission)

  | _ ->
    raise (Error "Grant")

let access_control_policy_of_xml = function
  | X.E ("AccessControlPolicy",_,[
    X.E ("Owner", _, owner_x);
    X.E ("AccessControlList", _, grants_x) ]) ->
    let owner = identity_of_xml owner_x in
    let grants = List.map grant_of_xml grants_x in
    new acl owner grants
  | _ ->
    raise (Error "AccessControlPolicy:t")

let get_bucket_acl creds region bucket =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`GET
    ~date
    ~bucket
    ~sub_resources:[`acl, None]
    creds
  in
  let headers = [ "Date", date ; authorization_header ] in
  let request_url = (service_url_of_region region) ^
    (Util.encode_url bucket) ^ "?" ^ (string_of_sub_resource `acl)
  in
  try_lwt
    lwt response_headers, response_body = HC.get ~headers request_url in
    return (`Ok (access_control_policy_of_xml (X.xml_of_string response_body)))
  with
    | HC.Http_error (404, _, _   ) -> return `NotFound
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  , _, body) -> error_msg body


(* set bucket acl *)
let xml_of_permission permission =
  X.E ("Permission",[],[X.P (string_of_permission permission)])

let ns_schema_instance = "http://www.w3.org/2001/XMLSchema-instance"
let ns_xmlns = "http://www.w3.org/2000/xmlns/"

let atts s = [
  (ns_xmlns          , "xsi" ), ns_schema_instance;
  (ns_schema_instance, "type"), s
]

let xml_of_identity = function
  | `amazon_customer_by_email email ->
    [X.E("EmailAddress",[],[X.P email])]

  | `canonical_user cn ->
    [X.E ("ID",[],[X.P cn#id]);
     X.E ("DisplayName",[],[X.P cn#display_name])]

  | `group group ->
    [X.E("URI",[],[X.P (string_of_group group)])]

let xml_of_grantee identity =
  let identity_x = xml_of_identity identity in
  X.E("Grantee", atts (tag_of_identity identity), identity_x)

let xml_of_owner identity =
  let identity_x = xml_of_identity identity in
  X.E ("Owner",[], identity_x)

let xml_of_grant (grantee, permission) =
  let kids = [xml_of_grantee grantee; xml_of_permission permission] in
  X.E("Grant", [], kids)

let xml_of_access_control_list grants =
  X.E("AccessControlList", [], List.map xml_of_grant grants)

let xml_of_access_control_policy acl =
  let kids = [
    xml_of_owner acl#owner ;
    xml_of_access_control_list acl#grants ]
  in
  X.E ("AccessControlPolicy", [], kids)

let xml_content_type_header = "Content-Type", "application/xml"
let json_content_type_header = "Content-Type", "application/json"

let set_bucket_acl creds region bucket acl  =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`PUT
    ~content_type:"application/xml"
    ~date
    ~bucket
    ~sub_resources:[`acl, None]
    creds
  in
  let request_url = (service_url_of_region region) ^
    (Util.encode_url bucket) ^ "?" ^ (string_of_sub_resource `acl)
  in
  let headers = [ "Date", date ; xml_content_type_header; authorization_header ] in
  let xml = xml_of_access_control_policy acl in
  let body = `String (X.string_of_xml xml) in
  try_lwt
    lwt _ = HC.put ~headers ~body request_url in
    return `Ok
  with
    | HC.Http_error (404, _, _   ) -> return `NotFound
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  , _, body) -> error_msg body

(* delete object *)
let delete_object creds region ~bucket ~objekt =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`DELETE
    ~date
    ~bucket
    ~request_uri:("/" ^ objekt)
    creds
  in
  let service_url = service_url_of_region region in
  let request_url = sprintf "%s%s/%s" service_url (Util.encode_url bucket)
    (Util.encode_url objekt)
  in
  let headers = [ "Date", date ; authorization_header ] in
  try_lwt
    lwt _ = HC.delete ~headers request_url in
    (* success actually signaled via a 204 *)
    fail (Error "delete_object")
  with
    | HC.Http_error (404, _, _   ) -> return `BucketNotFound
    | HC.Http_error (204, _, _   ) -> return `Ok
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  , _, body) -> error_msg body

(* get object acl *)
let get_object_acl creds region ~bucket ~objekt =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`GET
    ~date
    ~bucket
    ~request_uri:("/" ^ objekt)
    ~sub_resources:[`acl, None]
    creds
  in
  let headers = [ "Date", date ; authorization_header ] in
  let request_url = sprintf "%s%s/%s?%s" (service_url_of_region region)
    (Util.encode_url bucket) (Util.encode_url objekt)
    (string_of_sub_resource `acl)
  in
  try_lwt
    lwt response_headers, response_body = HC.get ~headers request_url in
    return (`Ok (access_control_policy_of_xml (X.xml_of_string response_body)))
  with
    | HC.Http_error (404, _, _   ) -> return `NotFound
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  , _, body) -> error_msg body

let set_object_acl creds region ~bucket ~objekt acl  =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`PUT
    ~content_type:"application/xml"
    ~date
    ~bucket
    ~request_uri:("/" ^ objekt)
    ~sub_resources:[`acl, None]
    creds
  in
  let request_url = sprintf "%s%s/%s?%s" (service_url_of_region region)
    (Util.encode_url bucket) (Util.encode_url objekt) (string_of_sub_resource `acl)
  in
  let headers = [ "Date", date ; xml_content_type_header; authorization_header ] in
  let xml = xml_of_access_control_policy acl in
  let body = `String (X.string_of_xml xml) in
  try_lwt
    lwt _ = HC.put ~headers ~body request_url in
    return `Ok
  with
    | HC.Http_error (404, _, _   ) -> return `NotFound
    | HC.Http_error (301, _, body) -> permanent_redirect_of_string body
    | HC.Http_error (_  , _ ,body) -> error_msg body

let get_bucket_policy creds region ~bucket =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`GET
    ~date
    ~bucket
    ~sub_resources:[`policy, None]
    creds
  in
  let request_url = sprintf "%s%s?%s" (service_url_of_region region)
    (Util.encode_url bucket) (string_of_sub_resource `policy)
  in
  let headers = [ "Date", date ; authorization_header ] in
  try_lwt
    lwt _, response_body = HC.get ~headers request_url in
    return (`Ok response_body)
  with
    | HC.Http_error (403, _, _   ) -> return `AccessDenied
    | HC.Http_error (405, _, body) -> return `NotOwner
    | HC.Http_error (404, _, body) -> return `NotFound
    | HC.Http_error (_  , _ ,body) -> error_msg body

let delete_bucket_policy creds region ~bucket =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`DELETE
    ~date
    ~bucket
    ~sub_resources:[`policy, None]
    creds
  in
  let request_url = sprintf "%s%s?%s" (service_url_of_region region)
    (Util.encode_url bucket) (string_of_sub_resource `policy)
  in
  let headers = [ "Date", date ; authorization_header ] in
  try_lwt
    HC.delete ~headers request_url >> return `Ok
  with
    | HC.Http_error (204, _, _) -> return `Ok

    | HC.Http_error (403, _, _   ) -> return `AccessDenied
    | HC.Http_error (405, _, body) -> return `NotOwner
    | HC.Http_error (_  , _ ,body) -> error_msg body

let set_bucket_policy creds region ~bucket ~policy =
  let date = now_as_string () in
  let authorization_header = auth_hdr
    ~http_method:`PUT
    ~date
    ~bucket
    ~sub_resources:[`policy, None]
    ~content_type:(snd json_content_type_header)
    creds
  in
  let request_url = sprintf "%s%s?%s" (service_url_of_region region)
    (Util.encode_url bucket) (string_of_sub_resource `policy)
  in
  let headers = [ "Date", date ; json_content_type_header; authorization_header ] in
  let body = `String policy in
  try_lwt
    lwt _ = HC.put ~headers ~body request_url in
    return `Ok
  with
    | HC.Http_error (204, _, _) -> return `Ok
    | HC.Http_error (403, _, _   ) -> return `AccessDenied
    | HC.Http_error (400, _, _) -> return `MalformedPolicy
    | HC.Http_error (_  , _ ,body) -> error_msg body

end

(* Copyright (c) 2011:

      barko 00336ea19fcb53de187740c490f764f4
      William Le Ferrand

   All rights reserved.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

   1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the
   distribution.

   3. Neither the name of barko nor the names of contributors may be used
   to endorse or promote products derived from this software without
   specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*)
