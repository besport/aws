
module Make = functor (HC : Aws_sigs.HTTP_CLIENT) ->
  struct

module Iam = IAM_factory.Make(HC)
open Lwt

module C = CalendarLib.Calendar
module P = CalendarLib.Printer.CalendarPrinter

open Creds
exception Service_down
exception Bad_response

type action =
  [ `BatchGetItem
  | `CreateTable
  | `DeleteItem
  | `DeleteTable
  | `DescribeTable
  | `GetItem
  | `ListTables
  | `PutItem
  | `Query
  | `Scan
  | `UpdateItem
  | `UpdateTable ]

let string_of_action = function
  | `BatchGetItem -> "BatchGetItem"
  | `CreateTable -> "CreateTable"
  | `DeleteItem -> "DeleteItem"
  | `DeleteTable -> "DeleteTable"
  | `DescribeTable -> "DescribeTable"
  | `GetItem -> "GetItem"
  | `ListTables -> "ListTables"
  | `PutItem -> "PutItem"
  | `Query -> "Query"
  | `Scan -> "Scan"
  | `UpdateItem -> "UpdateItem"
  | `UpdateTable -> "UpdateTable"

type attr =
  [ `Number
  | `String
  | `NumberSet
  | `StringSet ]


let string_of_attr = function
  | `Number -> "N" (*number*)
  | `NumberSet -> "NS" (*number set*)
  | `String -> "S" (*string*)
  | `StringSet -> "SS" (*string set*)

let attr_of_string = function
  | "N" -> `Number
  | "NS" -> `NumberSet
  | "S" -> `String
  | "SS" -> `StringSet
  | _ -> raise Not_found

type value =
  [ `Number of int64
  | `NumberSet of int64 list
  | `String of string
  | `StringSet of string list ]

let json_of_value = function
  | `Number n -> `Assoc ["N",`String (Int64.to_string n)]
  | `NumberSet ns -> `Assoc ["NS",`List (List.map (fun n -> `String (Int64.to_string n)) ns)]
  | `String s -> `Assoc ["S",`String s]
  | `StringSet ss -> `Assoc ["SS", `List (List.map (fun s -> `String s) ss)]

type key = string * attr

type update_action =
  [ `Put
  | `Delete
  | `Add ]

let string_of_update_action = function
  | `Put -> "PUT"
  | `Delete -> "DELETE"
  | `Add -> "ADD"

type token_state =
    Valid
  | Updating of unit Lwt.u list

type token = {
  mutable token_state: token_state;
  mutable token : Iam.session_token;
  aws_access_key_id : string;
  aws_secret_access_key : string;
}




let host = "dynamodb.us-east-1.amazonaws.com"

let create_token aws_access_key_id aws_secret_access_key =
  lwt token = Iam.get_session_token aws_access_key_id aws_secret_access_key in
  return {
    token;
    token_state=Valid;
    aws_access_key_id;
    aws_secret_access_key
  }

let get_token_value token = token.token

let make_headers ~token ?(version="20111205") action body =
  let rec loop () =
    if not(Iam.token_is_valid token.token)
    then
      begin
	match token.token_state with
	  | Updating l ->
	    let t,u = Lwt.wait () in
	    token.token_state <- Updating (u::l);
	    t >>= loop
	  | Valid ->
	    token.token_state <- Updating [];
	    try_lwt
	      begin
		(* debug "Dynamo: getting new session token"; *)
		lwt token_ = Iam.get_session_token token.aws_access_key_id token.aws_secret_access_key in
		let l = match token.token_state with | Updating l -> l | _ -> [] in
		token.token_state <- Valid;
		List.iter (fun x -> Lwt.wakeup x ()) l;
		loop ()
	      end
	    with _ ->
	      ((* debug "Dynamo: fail to get valid session token, retry later"; *)
	       Lwt_unix.sleep 0.1 >>= loop)
      end
    else
      let date =
        Aws_util.reset_calendar_lang ();
        P.sprint "%a, %d %b %Y %H:%M:%S GMT" (C.now ()) in
      let target = Printf.sprintf "DynamoDB_%s.%s" version (string_of_action action) in
      let headers_amz =
	[
	  "host",host;
	  "x-amz-date",date;
	  "x-amz-security-token",token.token.Iam.session_token;
	  "x-amz-target",target]
      in
      let cano =
	String.concat "" (List.map (fun (a,b) -> a^":"^b^"\n") headers_amz) in
      let string_to_sign =
	Printf.sprintf "POST\n/\n\n%s\n%s" cano body in
      let encoder = Cryptokit.Base64.encode_multiline () in
      let digester = Cryptokit.Hash.sha1 () in
      let hasher = Cryptokit.MAC.hmac_sha1 token.token.Iam.secret_access_key in
      let signature = Cryptokit.transform_string encoder (Cryptokit.hash_string hasher (Cryptokit.hash_string digester string_to_sign)) in
      let signature = String.sub signature 0 ((String.length signature) - 1) in
      let value = Printf.sprintf "AWS3 AWSAccessKeyId=%s,Algorithm=HmacSHA1,SignedHeaders=%s,Signature=%s"
	token.token.Iam.access_key_id (String.concat ";" (List.map fst headers_amz)) signature in
      let headers = ("x-amzn-authorization",value)::headers_amz in
      let headers = ("Content-Type","application/x-amz-json-1.0")::headers in
      return headers
 in loop ()

let post ~token action json =
  let content =Yojson.Safe.to_string json in
  lwt headers = make_headers ~token action content in
  lwt _,s = HC.post ~headers ~body:(`String content) (Printf.sprintf "http://%s/" host) in
  try_lwt
    Lwt.return (Yojson.Safe.from_string s)
  with _ -> return (`String "")

let assoc_none_if_empty name :(string * Yojson.Safe.json)list -> (string * Yojson.Safe.json) option  = function
  | [] -> None
  | l -> Some (name,`Assoc l)

let list_none_if_empty name = function
  | [] -> None
  | l -> Some (name,`List l)

let filter_map_opt name f l =
  match Aws_util.filter_map f l with
    | [] -> None
    | l -> Some (name,`Assoc l)

let string_of_json_string : Yojson.Safe.json -> string = function
  | `String s -> s
  | _ -> raise Bad_response

let string_list_of_json_list : Yojson.Safe.json -> string list = function
  | `List l -> List.map string_of_json_string l
  | _ -> raise Bad_response

let ident x = x


type table_status = [`Creating | `Active | `Deleting | `Updating ]

type table_description = {
  name : string;
  count : int;
  key :(string * attr);
  range : (string * attr) option;
  size : int;
  status : table_status;
  write_limit : int;
  read_limit : int;
  creation : int64;
}
type scan_result = {
  res_count : int;
  res_items : (string * value) list list;
  res_last : (key * key option) option;
  res_consumed : float;
  res_scanned_count : int option;
}

let table_status_of_string = function
  | "CREATING" -> `Creating
  | "ACTIVE" -> `Active
  | "DELETING" -> `Deleting
  | "UPDATING" -> `Updating
  | _ -> raise Not_found

let string_of_table_status = function
  | `Creating -> "CREATING"
  | `Active -> "ACTIVE"
  | `Deleting -> "DELETING"
  | `Updating -> "UPDATING"

let get_string f = function `String s -> f s | _ -> raise Not_found

let get_float = function `Float f -> f | _ -> raise Not_found

let get_list_of_assoc l name =
  try
    match List.assoc name l with
      | `Assoc l -> l
      | _ -> []
  with _ -> []

let get_list l name =
  try
    match List.assoc name l with
      | `List l -> l
      | _ -> []
  with _ -> []


let get_int = function `Int i -> i | _ -> raise Not_found

let json_to_key = function
  | `Assoc l -> let name = List.assoc "AttributeName" l in
		let typ = List.assoc "AttributeType" l in
		get_string ident name, (get_string attr_of_string typ)
  | _ -> raise Not_found

let table_description_of_json = function
  | `Assoc l ->
    let opt l name f default = try f (List.assoc name l) with _ -> match default with | Some x -> x | _ -> raise Not_found in

    let limits = get_list_of_assoc l "ProvisionedThroughput" in
    let schema = get_list_of_assoc l "KeySchema" in
    begin
      try
	return {
	  name  = opt l "TableName" (get_string ident) None;
	  count = opt l "ItemCount" get_int (Some 0) ;
	  size  = opt l "TableSizeBytes" get_int (Some 0);
	  write_limit = opt limits "ReadCapacityUnits" get_int (Some 0);
	  read_limit  = opt limits "WriteCapacityUnits" get_int (Some 0);
	  status = opt l "TableStatus" (get_string table_status_of_string) None;
	  creation = opt l "CreationDateTime" (get_string (fun s -> Int64.of_float (float_of_string s))) (Some 0L);
	  key = opt schema "HashKeyElement" json_to_key None;
	  range = try Some (opt schema "RangeKeyElement" json_to_key None) with _ -> None;
	}
      with _ -> fail Bad_response
    end
  | _ -> fail Bad_response

let get_string = function
  | `String s-> s
  | _ -> raise Not_found

let get_string_list = function
  | `List l -> List.map get_string l
  | _ -> raise Not_found

let value_of_json = function
  | `Assoc ["SS",l] -> `StringSet (get_string_list l)
  | `Assoc ["S",l] -> `String (get_string l)
  | `Assoc ["NS",l] -> `NumberSet (List.map Int64.of_string (get_string_list l))
  | `Assoc ["N",l] -> `Number (Int64.of_string (get_string l))
  | _ -> raise Not_found

let item_of_json = function
  | `Assoc l -> List.map (fun (name,json) -> name,value_of_json json) l
  | _ -> raise Not_found

open Yojson.Safe

(** Api **)

(** Error **)
exception Error of (string * string)
exception UnknownError of json
let error_of_json = function
  | `Assoc l as json -> (
    try
      let t = match List.assoc "__type" l with
	| `String s ->
	  begin
	    try
	      let i = String.index s '#' in
	      String.sub s (i+1) ((String.length s) - i - 1)
	    with _ -> s
	  end
	| _ -> "" in
      let m = match List.assoc "message" l with
	| `String s -> s
	| _ -> ""
      in fail (Error (t,m))
    with _ -> fail (UnknownError json))
  | json -> fail (UnknownError json)

(** Tables **)

let create_table ~token ~table ~key ?range ~readlimit ~writelimit () =
  let make_key name (n,t) =
    name,`Assoc [
      "AttributeName",`String n;
      "AttributeType",`String (string_of_attr t)
    ] in
  let schema =
    let key = Some (make_key "HashKeyElement" key) in
    let range = Aws_util.option_map (make_key "RangeKeyElement") range in
    filter_map_opt "KeySchema" ident [key;range] in
  let make_limit name i = Some(name,`Int i) in
  let limits =
    filter_map_opt
      "ProvisionedThroughput"
      ident
      [make_limit "ReadCapacityUnits" readlimit;
       make_limit "WriteCapacityUnits" writelimit] in
  let json = `Assoc (Aws_util.filter_map ident [Some("TableName", `String table);schema;limits]) in
  lwt json = post ~token `CreateTable json in
  match json with
    | `Assoc ["TableDescription",json] -> table_description_of_json json
    | _ -> error_of_json json

let delete_table ~token ~table =
  let json = `Assoc ["TableName", `String table] in
  lwt json = post ~token `DeleteTable json in
  match json with
    | `Assoc ["TableDescription",json] -> table_description_of_json json
    | _ -> error_of_json json

let update_table ~token ~table ?readlimit ?writelimit () =
  let make_limit name = Aws_util.option_map (fun i -> name,`Int i) in
  let limits =
    filter_map_opt
      "ProvisionedThroughput"
      ident
      [make_limit "ReadCapacityUnits" readlimit;
       make_limit "WriteCapacityUnits" writelimit] in
  let json = `Assoc (Aws_util.filter_map ident [Some("TableName", `String table);limits]) in
  lwt json = post ~token `UpdateTable json in
  match json with
    | `Assoc ["TableDescription",json] -> table_description_of_json json
    | _ -> error_of_json json


let describe_table ~token ~table =
  lwt json = post ~token `DescribeTable (`Assoc ["TableName",`String table]) in
  match json with
    | `Assoc ["Table",json] -> table_description_of_json json
    | _ -> error_of_json json

let list_tables ~token ?limit ?from () =
  let json = `Assoc (Aws_util.filter_map ident [
    Aws_util.option_map (fun i -> "Limit", `Int i) limit;
    Aws_util.option_map (fun from -> "ExclusiveStartTableName", `String from) from;
  ])
  in
  match_lwt post ~token `ListTables json with
    | `Assoc l ->
      let last = try Some (string_of_json_string (List.assoc "LastEvaluatedTableName" l)) with _ -> None in
      (try
	 let l =  List.assoc "TableNames" l
	 in return ((string_list_of_json_list l),last)
       with _ -> fail Bad_response)
    | _ -> fail Bad_response

(** Items **)

let put_item ~token ~table ~item ?(expect=[]) ?(return=false) () =
  let make_item (name,v) : string * json =
    name,`Assoc [
      "Value",json_of_value v;
    ] in
  let item : (string * json) option = assoc_none_if_empty "Item" [make_item item] in
  let expect =
    List.map (fun (e,p) -> e,`Assoc (match p with
      | `Exists ->        ["Exists",`Bool true]
      | `StringValue s -> ["Value",`Assoc ["S",`String s]]
      | `IntValue s ->    ["Value",`Assoc ["I",`String s]])) expect in
  let expect = assoc_none_if_empty "Expected" expect in
  let return = if return then Some ("ReturnValues",`String "ALL_OLD") else None in
  let json = `Assoc (Aws_util.filter_map ident [Some("TableName", `String table);item;expect;return]) in
  match_lwt post ~token `PutItem json with
    | `Assoc l ->
      let value = try Some (item_of_json (List.assoc "Attributes" l)) with _ -> None in
      let consum = get_float (List.assoc "ConsumedCapacityUnits" l) in
      Lwt.return (consum,value)
    | json -> error_of_json json


let get_item ~token table key ?range ?(select=[]) ?consistent () =
  let make_key name (n,t) =
    name,`Assoc [
      string_of_attr t,`String n;
    ] in
  let keys =
    let key = Some (make_key "HashKeyElement" key) in
    let range = Aws_util.option_map (make_key "RangeKeyElement") range in
    filter_map_opt "Key" ident [key;range] in
  let get = list_none_if_empty "AttributesToGet" (List.map (fun x -> `String x) select) in
  let consistent = Aws_util.option_map (fun b -> "ConsistentRead", `Bool b) consistent in
  let json = `Assoc (Aws_util.filter_map ident [Some("TableName", `String table);keys;get;consistent]) in
  match_lwt post ~token `GetItem json with
    | `Assoc l ->
      let value = item_of_json (List.assoc "Item" l) in
      let consum = get_float (List.assoc "ConsumedCapacityUnits" l) in
      Lwt.return (consum,value)
    | json -> error_of_json json

let batch_get_item ~token items =
  let make_key name (v,t) =
    name,`Assoc [
      string_of_attr t,`String v;
    ] in
  let make_keys (p,r) =
    let key = Some (make_key "HashKeyElement" p) in
    let range = Aws_util.option_map (make_key "RangeKeyElement") r in
    `Assoc (Aws_util.filter_map ident [key;range]) in
  let json = `Assoc ["RequestItems",`Assoc
    (List.map (fun (table,keylist,getlist) ->
      table,
      `Assoc (Aws_util.filter_map ident
		[
		  list_none_if_empty "Keys" (List.map make_keys keylist);
		  list_none_if_empty "AttributesToGet" (List.map (fun x -> `String x) getlist)
		]
      )
     ) items)] in
  match_lwt post ~token `BatchGetItem json with
    | `Assoc ["Responses",`Assoc l] ->
      let get_table (name,items) =
	match items with
	  | `Assoc l ->
	    let values = match List.assoc "Items" l with
	      | `List l -> List.map item_of_json l
	      | _ -> raise Not_found in
	    let consum = get_string (List.assoc "ConsumedCapacityUnits" l) in
	    return (name,consum,values)
	  | json -> error_of_json json in
      Lwt_list.map_s get_table l
    | json -> error_of_json json

let update_item ~token ~table ~key ?range update ?(expect=[]) ?(return=false) () =
  match update with
    | [] -> Lwt.return (0.,None)
    | update ->
      let make_key name v =
	name, json_of_value v in
      let keys =
	let key = Some (make_key "HashKeyElement" key) in
	let range = Aws_util.option_map (make_key "RangeKeyElement") range in
	filter_map_opt "Key" ident [key;range] in
      let expect =
	List.map (fun (e,p) -> e,`Assoc (match p with
	  | `Exists ->        ["Exists",`Bool true]
	  | `StringValue s -> ["Value",`Assoc ["S",`String s]]
	  | `IntValue s ->    ["Value",`Assoc ["I",`String s]])) expect in
      let expect = assoc_none_if_empty "Expected" expect in
      let return = if return then Some ("ReturnValues",`String "ALL_OLD") else None in
      let make_update (attr_name,value) =
	attr_name,
	`Assoc [
	  match value with
	    | `Delete -> "Action", `String (string_of_update_action `Delete)
	    | `Number _ | `NumberSet _ | `String _ | `StringSet _ as value -> "Value",json_of_value value;
    (* "Action",`String (string_of_update_action action); *)
	] in
      let update = assoc_none_if_empty "AttributeUpdates" (List.map make_update update) in
      let json = `Assoc (Aws_util.filter_map ident [Some ("TableName",`String table);keys;update;expect;return]) in
      (* debug "update_item: %s" (Yojson.Safe.to_string json); *)
      match_lwt post ~token `UpdateItem json with
	| `Assoc l ->
	  let value = try Some (item_of_json (List.assoc "Attributes" l)) with _ -> None in
	  let consum = get_float (List.assoc "ConsumedCapacityUnits" l) in
	  Lwt.return (consum,value)
	| json -> error_of_json json

let delete_item ~token table key ?range ?(expect=[]) ?(return=false) () =
  let make_key name (n,t) =
    name,`Assoc [
      string_of_attr t,`String n;
    ] in
  let keys =
    let key = Some (make_key "HashKeyElement" key) in
    let range = Aws_util.option_map (make_key "RangeKeyElement") range in
    filter_map_opt "Key" ident [key;range] in
  let expect =
    List.map (fun (e,p) -> e,`Assoc (match p with
      | `Exists ->        ["Exists",`Bool true]
      | `StringValue s -> ["Value",`Assoc ["S",`String s]]
      | `IntValue s ->    ["Value",`Assoc ["I",`String s]])) expect in
  let expect = assoc_none_if_empty "Expected" expect in
  let return = if return then Some ("ReturnValues",`String "ALL_OLD") else None in
  let json = `Assoc (Aws_util.filter_map ident [Some("TableName", `String table);keys;expect;return]) in
  match_lwt post ~token `DeleteItem json with
    | `Assoc l ->
      let value = try Some (item_of_json (List.assoc "Attributes" l)) with _ -> None in
      let consum = get_float (List.assoc "ConsumedCapacityUnits" l) in
      Lwt.return (consum,value)
    | json -> error_of_json json


(** Query **)

(** Scan **)

let scan ~token ~table ?(select=[]) ?limit ?from ?count () =
  let make_key (n,t) = `Assoc [ string_of_attr t,`String n ] in
  let json = `Assoc (Aws_util.filter_map ident [
    Some ("TableName",`String table);
    list_none_if_empty "AttributesToGet" (List.map (fun x -> `String x) select);
    Aws_util.option_bind (fun c -> if c then Some ("Count",`Bool true) else None ) count;
    Aws_util.option_map (fun i -> "Limit", `Int i) limit;
    Aws_util.option_map (fun (from,range) -> "ExclusiveStartKey", `Assoc (
      Aws_util.filter_map ident [
	Some ("HashKeyElement", make_key from);
	Aws_util.option_map (fun range -> "RangeKeyElement",make_key range) range;
      ])) from;
  ])
  in
  match_lwt post ~token `Scan json with
    | `Assoc l ->
      let res_last =
	try
	  let last = get_list_of_assoc l "LastEvaluatedKey" in
	  let key = json_to_key (List.assoc "HashKeyElement" last) in
	  let range = try Some (json_to_key (List.assoc "RangeKeyElement" last)) with _ -> None in
	  Some (key,range)
	with _ -> None in
      let res_count = get_int (List.assoc "Count" l) in
      let res_items = List.map item_of_json (get_list l "Items") in
      let res_scanned_count = try Some (get_int (List.assoc "ScannedCount" l)) with _ -> None in
      let res_consumed = get_float (List.assoc "ConsumedCapacityUnits" l) in
      return {res_count;res_items;res_last;res_consumed;res_scanned_count}
    | _ -> fail Bad_response


end
