
module Make = functor (HC : Aws_sigs.HTTP_CLIENT) ->
  struct


module X = My_xml


open Lwt
open Creds

module Util = Aws_util

exception Error of string

let sprint = Printf.sprintf

(* convenience functions for navigating xml nodes *)
let find_kids e_list k =
  try
    let el = List.find (function
      | X.E( name, _, kids ) -> name = k
      | X.P _ -> false
    ) e_list in
    let kids =
      match el with
        | X.E( name, _, kids ) -> kids
        | X.P _ -> assert false
    in
    Some kids
  with Not_found ->
    None

let find_kids_else_error e_list k =
  match find_kids e_list k with
    | None -> raise (Error k)
    | Some kids -> kids

let find_p_kid e_list k =
  match find_kids e_list k with
    | None -> None
    | Some [X.P p_kid] -> Some p_kid
    | _ -> None

let find_e_kid e_list k =
  match find_kids e_list k with
    | None -> None
    | Some [e] -> Some e
    | _ -> None

let find_p_kid_else_error e_list k =
  match find_p_kid e_list k with
    | None -> raise (Error k)
    | Some p -> p

let find_e_kid_else_error e_list k =
  match find_e_kid e_list k with
    | None -> raise (Error k)
    | Some e -> e


(* compute the AWS SHA1 signature that to annotate a Query-style request *)
let signed_request
    ?region
    ?(http_method=`GET)
    ?(http_uri="/")
    ?expires_minutes
    creds
    params  =

  let http_host =
    match region with
      | Some r -> sprint "ec2.%s.amazonaws.com" r
      | None -> "ec2.amazonaws.com"
  in

  let params =
    ("Version", "2010-08-31" ) ::
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
  let params_s = String.concat "&" (Util.encode_key_equals_value params) in
  sprint "http://%s%s?%s" http_host http_uri params_s


(* describe regions *)
let item_of_xml = function
  | X.E("item",_,[
    X.E("regionName",_,[X.P name]);
    X.E("regionEndpoint",_,[X.P endpoint])
  ]) -> name, endpoint
  | _ -> raise (Error "DescribeRegionsResponse.RegionInfo.item")

let describe_regions_response_of_xml = function
  | X.E("DescribeRegionsResponse", _, kids) -> (
    match kids with
      | [_ ; X.E ("regionInfo",_,items_x)] -> (
        List.map item_of_xml items_x
      )
      | _ -> raise (Error "DescribeRegionsResponse.regionInfo")
  )
  | _ -> raise (Error "DescribeRegionsResponse")

let describe_regions ?expires_minutes creds =
  let request = signed_request creds ?expires_minutes
    ["Action", "DescribeRegions" ] in
  lwt header, body = HC.get request in
  let xml = X.xml_of_string body in
  return (describe_regions_response_of_xml xml)

(* describe spot price history *)
let item_of_xml = function
  | X.E ("item",_,[
    X.E ("instanceType",_,[X.P instance_type]);
    X.E ("productDescription",_,[X.P product_description]);
    X.E ("spotPrice",_,[X.P spot_price_s]);
    X.E ("timestamp",_,[X.P timestamp_s])
  ]) ->

    let spot_price = float_of_string spot_price_s in
    let timestamp = Util.unixfloat_of_amz_date_string timestamp_s in
    (object
      method instance_type = instance_type
      method product_description = product_description
      method spot_price = spot_price
      method timestamp = timestamp
     end)

  | _ -> raise (Error (String.concat "." [
    "DescribeSpotPriceHistoryResponse";
    "spotPriceHistorySet";
    "item"
  ]))


let describe_spot_price_history_of_xml = function
  | X.E ("DescribeSpotPriceHistoryResponse",_,kids) -> (
    match kids with
      | [_ ; X.E("spotPriceHistorySet",_,kids) ] ->
        List.map item_of_xml kids

      | _ ->
        raise (
          Error ("DescribeSpotPriceHistoryResponse." ^
            "spotPriceHistorySet")
        )
  )
  | _ ->
    raise (Error "DescribeSpotPriceHistoryResponse")

let filters_args kv_list =
  let _, f = List.fold_left (
    fun (c,accu) (k,v) ->
      let kh = sprint "Filter.%d.Name" c, k in
      let vh = sprint "Filter.%d.Value" c, v in
      c+1, kh :: vh :: accu
  ) (1,[]) kv_list
  in
  f

type instance_type = [
| `m1_small
| `m1_large
| `m1_xlarge
| `c1_medium
| `c1_xlarge
| `m2_xlarge
| `m2_2xlarge
| `m2_4xlarge
| `cc1_4xlarge
| `cg1_4xlarge
| `t1_micro
]

let string_of_instance_type = function
  | `m1_small       -> "m1.small"
  | `m1_large       -> "m1.large"
  | `m1_xlarge      -> "m1.xlarge"
  | `c1_medium      -> "c1.medium"
  | `c1_xlarge      -> "c1.xlarge"
  | `m2_xlarge      -> "m2.xlarge"
  | `m2_2xlarge     -> "m2.2xlarge"
  | `m2_4xlarge     -> "m2.4xlarge"
  | `cc1_4xlarge    -> "cc1.4xlarge"
  | `cg1_4xlarge    -> "cg1.4xlarge"
  | `t1_micro       -> "t1.micro"

let instance_type_of_string = function
  | "m1.small"    -> Some `m1_small
  | "m1.large"    -> Some `m1_large
  | "m1.xlarge"   -> Some `m1_xlarge
  | "c1.medium"   -> Some `c1_medium
  | "c1.xlarge"   -> Some `c1_xlarge
  | "m2.xlarge"   -> Some `m2_xlarge
  | "m2.2xlarge"  -> Some `m2_2xlarge
  | "m2.4xlarge"  -> Some `m2_4xlarge
  | "cc1.4xlarge" -> Some `cc1_4xlarge
  | "cg1.4xlarge" -> Some `cg1_4xlarge
  | "t1.micro"    -> Some `t1_micro
  | _ -> None

let describe_spot_price_history ?expires_minutes ?region ?instance_type creds  =
  let args =
    match instance_type with
      | Some it -> filters_args ["instance-type", string_of_instance_type it ]
      | None -> []
  in
  let request = signed_request creds ?region ?expires_minutes
    (("Action", "DescribeSpotPriceHistory") :: args)
  in
  lwt header, body = HC.get request in
  let xml = X.xml_of_string body in
  return (describe_spot_price_history_of_xml xml)

(* terminate instances *)

(* from:
   http://docs.amazonwebservices.com/AWSEC2/latest/APIReference/index.html?ApiReference-ItemType-InstanceStateType.html
*)

type instance_state = [
| `pending
| `running
| `shutting_down
| `terminated
| `stopping
| `stopped
]

let instance_state_of_code code =
  (* only low byte meaningful *)
  match code land 0xff with
    |  0  -> `pending
    | 16  -> `running
    | 32  -> `shutting_down
    | 48  -> `terminated
    | 64  -> `stopping
    | 80  -> `stopped
    | x   -> raise (Error (Printf.sprintf "instance_state_of_code: %d" x))

let string_of_instance_state = function
  | `pending       -> "pending"
  | `running       -> "running"
  | `shutting_down -> "shutting-down"
  | `terminated    -> "terminated"
  | `stopping      -> "stopping"
  | `stopped       -> "stopped"


let state_of_xml = function
  | [ X.E ("code",_,[X.P code_s]);
      X.E ("name",_,[X.P name])
    ] ->
    instance_state_of_code (int_of_string code_s)
  | _ ->
    raise (Error "state")

let item_of_xml = function
  | X.E ("item",_, [
    X.E ("instanceId",_,[X.P instance_id]);
    X.E ("currentState",_,c_state_x);
    X.E ("previousState",_,p_state_x)
  ]) ->
    let current_state = state_of_xml c_state_x in
    let previous_state = state_of_xml p_state_x in
    (object
      method instance_id = instance_id
      method current_state = current_state
      method previous_state = previous_state
     end)

  | _ ->
    raise (Error "TerminateInstancesResponse.instancesSet.item")


let terminate_instances_of_xml = function
  | X.E ("TerminateInstancesResponse",_, [
    _request_id_x;
    X.E ("instancesSet",_, items)
  ]) ->
    List.map item_of_xml items
  | _ -> raise (Error "TerminateInstancesResponse")

type error = [
| `Error of (string * string) (* [code,message] *)
| `InsufficientInstanceCapacity of string
]

 (* generic error *)
let error_msg body =
  match X.xml_of_string body with
    | X.E ("Response",_,(X.E ("Errors",_,[X.E ("Error",_,[
      X.E ("Code",_,[X.P code]);
      X.E ("Message",_,[X.P message])
    ])]))::_) -> (
        match code with
          | "InsufficientInstanceCapacity" ->
              `InsufficientInstanceCapacity message
          | _ ->
              (* haven't cataloged' all error messages *)
              `Error (code, message)
      )

  | _ -> raise (Error "Response.Errors.Error")

let instance_id_args instance_ids =
  Util.list_map_i (
    fun i instance_id ->
      sprint "InstanceId.%d" (i+1), instance_id
  ) instance_ids

let terminate_instances ?expires_minutes ?region creds instance_ids =
  let args = instance_id_args instance_ids in
  let request = signed_request creds ?region ?expires_minutes
    (("Action", "TerminateInstances") :: args)
  in
  try_lwt
    lwt header, body = HC.get request in
    let xml = X.xml_of_string body in
    return (`Ok  (terminate_instances_of_xml xml))
  with
    | HC.Http_error (_,_,body) ->
        return (error_msg body)

(* describe instances *)
type instance = <
  id : string;
  ami_launch_index : int;
  architecture_opt : string option;
  placement_availability_zone_opt : string option;
  dns_name_opt : string option;
  placement_group_opt : string option;
  image_id : string;
  instance_type : instance_type;
  ip_address_opt : string option;
  kernel_id_opt : string option;
  key_name_opt : string option;
  launch_time : float;
  lifecycle_opt : string option;
  private_dns_name_opt : string option;
  private_ip_address_opt : string option;
  ramdisk_id_opt: string option;
  reason_opt : string option;
  root_device_name_opt : string option;
  root_device_type : string;
  state : instance_state;
  virtualization_type_opt : string option;
  monitoring : string
>

type reservation = <
  id : string;
  groups : string list;
  owner_id : string;
  instances : instance list
>

let group_of_xml = function
  | X.E("item",_,[X.E("groupId",_,[X.P group])]) -> group
  | _ ->
    raise (Error (
      String.concat "." [
        "DescribeInstancesResponse";
        "reservationSet";
        "item";
        "groupSet";
        "item"]
    ))

let placement_of_xml kids =
  let availability_zone_opt = find_p_kid kids "availabilityZone" in
  let group_name_opt = find_p_kid kids "groupName" in
  availability_zone_opt, group_name_opt

let instance_of_xml = function
  | X.E("item",_,kids) ->

    let fp = find_p_kid_else_error kids in
    let fpo = find_p_kid kids in

    let id = fp "instanceId" in
    let image_id = fp "imageId" in
    let state_x = find_kids_else_error kids "instanceState" in
    let private_dns_name_opt = fpo "privateDnsName" in
    let dns_name_opt = fpo "dnsName" in
    let key_name_opt = fpo "keyName" in
    let ami_launch_index_s = fp "amiLaunchIndex" in
    let instance_type =
      match instance_type_of_string (fp "instanceType") with
        | Some it -> it
        | None -> raise (Error "instance_type")
    in
    let launch_time_s = fp "launchTime" in
    let placement_x = find_kids_else_error kids "placement" in
    let kernel_id_opt = fpo "kernelId" in
    let virtualization_type_opt = fpo "virtualizationType" in
    let private_ip_address_opt = fpo "privateIpAddress" in
    let ip_address_opt = fpo "ipAddress" in
    let architecture_opt = fpo "architecture" in
    let root_device_type = fp "rootDeviceType" in
    let root_device_name_opt = find_p_kid kids "rootDeviceName" in
    let reason_opt = fpo "reason" in
    let ramdisk_id_opt = fpo "ramdiskId" in
    let lifecycle_opt = fpo "instanceLifecycle" in
    let monitoring =
      match find_kids kids "monitoring" with
        | Some [X.E ("state",_,[X.P monitoring_state])] -> monitoring_state
        | _ -> raise (Error "monitoring")
    in

    (* TODO:
       product_code
       block_device_mapping
       client_token
       tags
       product_codes
       block_device_mapping
       spot_instance_request_id
    *)

    let state = state_of_xml state_x in
    let ami_launch_index = int_of_string ami_launch_index_s in
    let launch_time = Util.unixfloat_of_amz_date_string launch_time_s in
    let placement_availability_zone_opt, placement_group_opt =
      placement_of_xml placement_x in
    (object
      method id = id
      method image_id = image_id
      method state = state
      method private_dns_name_opt = private_dns_name_opt
      method dns_name_opt = dns_name_opt
      method reason_opt = reason_opt
      method key_name_opt = key_name_opt
      method ami_launch_index = ami_launch_index
      method instance_type = instance_type
      method launch_time = launch_time
      method placement_availability_zone_opt = placement_availability_zone_opt
      method placement_group_opt = placement_group_opt
      method kernel_id_opt = kernel_id_opt
      method ramdisk_id_opt = ramdisk_id_opt
      method private_ip_address_opt = private_ip_address_opt
      method ip_address_opt = ip_address_opt
      method architecture_opt = architecture_opt
      method root_device_type = root_device_type
      method root_device_name_opt = root_device_name_opt
      method lifecycle_opt = lifecycle_opt
      method virtualization_type_opt = virtualization_type_opt
      method monitoring = monitoring
     end)
  | _ ->
    raise (Error "DescribeInstancesResponse.reservationSet.item.instancesSet")

let reservation_of_xml kids =
  let fp = find_p_kid_else_error kids in
  let fe = find_kids_else_error kids in
  let reservation_id = fp "reservationId" in
  let owner_id = fp "ownerId" in
  let groups_x = fe "groupSet" in
  let instances_x = fe "instancesSet" in
  let groups = List.map group_of_xml groups_x in
  let instances = List.map instance_of_xml instances_x in
  (object
    method id = reservation_id
    method owner_id = owner_id
    method groups = groups
    method instances = instances
   end)


let reservation_item_of_xml = function
  | X.E("item",_,reservation_x) -> reservation_of_xml reservation_x
  | _ -> raise (Error "DescribeInstancesResponse.reservationSet.item")

let describe_instances_of_xml = function
  | X.E("DescribeInstancesResponse",_,kids) -> (
    match kids with
      | [_; X.E("reservationSet",_,reservation_items)] ->
        List.map reservation_item_of_xml reservation_items
      | _ ->
        raise (Error "DescribeInstancesResponse.reservationSet")
  )
  | _ ->
    raise (Error "DescribeInstancesResponse")

let describe_instances ?expires_minutes ?region creds instance_ids =
  let args = instance_id_args instance_ids in
  let request = signed_request creds ?expires_minutes ?region
    (("Action", "DescribeInstances") :: args)
  in
  try_lwt
    lwt header, body = HC.get request in
    let xml = X.xml_of_string body in
    return (`Ok (describe_instances_of_xml xml))
  with
    | HC.Http_error (_,_,body) ->
      return (error_msg body)

(* run instances *)
let run_instances_of_xml = function
  | X.E("RunInstancesResponse",_, reservation_x) ->
    reservation_of_xml reservation_x
  | _ ->
    raise (Error "RunInstancesResponse")

let augment_opt f x = function
  | None -> x
  | Some y -> (f y) :: x

let run_instances
    ?expires_minutes
    ?key_name
    ?placement_availability_zone
    ?region
    ?placement_group
    ?instance_type
    ?user_data
    ?(security_groups=[]) (* secruity group names, not id's *)
    creds
    ~image_id
    ~min_count
    ~max_count =
  let args = [
    "Action", "RunInstances" ;
    "MinCount", string_of_int min_count ;
    "MaxCount", string_of_int max_count ;
    "ImageId", image_id
  ]
  in
  let args = augment_opt (fun az -> "Placement.AvailabilityZone", az)
    args placement_availability_zone in
  let args = augment_opt (fun pg -> "Placement.GroupName", pg)
    args placement_group in

  let args = augment_opt (fun ud -> "UserData", Util.base64 ud)
    args user_data in

  let args = augment_opt (fun kn -> "KeyName", kn) args key_name in
  let args = augment_opt (fun it -> "InstanceType", string_of_instance_type it)
    args instance_type in
  let sg = Util.list_map_i (
    fun i security_group -> sprint "SecurityGroup.%d" i, security_group ) security_groups in
  let args = args @ sg in

  let request = signed_request creds ?expires_minutes ?region args in
  try_lwt
    lwt header, body = HC.get request in
    let xml = X.xml_of_string body in
    return (`Ok (run_instances_of_xml xml))
  with
    | HC.Http_error (_,_,body) ->
      return (error_msg body)

(* request spot instances *)
type spot_instance_request_type = [`OneTime | `Persistent]
let string_of_spot_instance_request_type = function
  | `OneTime    -> "one-time"
  | `Persistent -> "persistent"

let spot_instance_request_type_of_string = function
  | "one-time"   -> `OneTime
  | "persistent" -> `Persistent
  | _ -> raise (Error "spot instance request type")

(* request spot instance *)
type spot_instance_request = {
  sir_spot_price : float ;
  sir_instance_count : int option;
  sir_type : spot_instance_request_type option;
  sir_valid_from : float option;
  sir_valid_until: float option;
  sir_launch_group : string option;
  sir_image_id : string ;
  sir_security_group : string option ;
  sir_user_data : string option;
  sir_instance_type : instance_type option;
  sir_kernel_id : string option;
  sir_ramdisk_id : string option;
  sir_availability_zone : string option;
  (* ? ls_device_name : string option *)
  sir_monitoring_enabled : bool option;
  sir_key_name : string option;
  sir_availability_zone_group : string option;
  sir_placement_group : string option;
}

let minimal_spot_instance_request ~spot_price ~image_id = {
  sir_spot_price = spot_price;
  sir_instance_count = None;
  sir_type = None;
  sir_valid_from = None;
  sir_valid_until = None;
  sir_launch_group = None;
  sir_image_id = image_id;
  sir_security_group = None;
  sir_user_data = None;
  sir_instance_type = None;
  sir_kernel_id = None;
  sir_ramdisk_id = None;
  sir_availability_zone = None;
  sir_monitoring_enabled = None;
  sir_key_name = None;
  sir_availability_zone_group = None;
  sir_placement_group = None
}


let spot_instance_request_args sir =
  let args = ref [] in
  let add k f = function
    | Some x -> args := (k, f x) :: !args
    | None -> ()
  in
  let addid k = add k (fun s -> s) in
  add "SpotPrice" string_of_float (Some sir.sir_spot_price);
  add "InstanceCount" string_of_int sir.sir_instance_count;
  add "Type" string_of_spot_instance_request_type sir.sir_type;
  add "ValidFrom" Util.amz_date_string_of_unixfloat sir.sir_valid_from;
  add "ValidUntil" Util.amz_date_string_of_unixfloat sir.sir_valid_until;
  addid "LaunchGroup" sir.sir_launch_group;
  addid "LaunchSpecification.KeyName" sir.sir_key_name;
  addid "LaunchSpecification.ImageId" (Some sir.sir_image_id);
  addid "LaunchSpecification.SecurityGroup" sir.sir_security_group;
  add "LaunchSpecification.UserData" Util.base64 sir.sir_user_data;
  add "LaunchSpecification.InstanceType" string_of_instance_type sir.sir_instance_type;
  addid "LaunchSpecification.KernelId" sir.sir_kernel_id;
  addid "LaunchSpecification.RamdiskId" sir.sir_ramdisk_id;
  addid "LaunchSpecification.Placement.AvailabilityZone" sir.sir_availability_zone;
  addid "LaunchSpecification.Placement.GroupName" sir.sir_placement_group;
  (* not documented! *)
  add "LaunchSpecification.Monitoring.Enabled" string_of_bool sir.sir_monitoring_enabled;
  addid "AvailabilityZoneGroup" sir.sir_availability_zone_group;
  !args


type spot_instance_request_state = [ `Active | `Open | `Closed | `Cancelled | `Failed ]
let string_of_spot_instance_request_state = function
  | `Active    -> "active"
  | `Open      -> "open"
  | `Closed    -> "closed"
  | `Cancelled -> "cancelled"
  | `Failed    -> "failed"

let spot_instance_request_state_of_string = function
  | "active"    -> `Active
  | "open"      -> `Open
  | "closed"    -> `Closed
  | "cancelled" -> `Cancelled
  | "failed"    -> `Failed
  | _ -> raise (Error "spot instance request state")

type spot_instance_request_description = <
  id : string;
  instance_id_opt : string option;
  sir_type : spot_instance_request_type ;
  spot_price : float;
  state : spot_instance_request_state;
  image_id_opt : string option;
  key_name_opt : string option;
  groups : string list;
  placement_group_opt : string option
>

let spot_instance_request_of_xml = function
  | X.E("item",_,kids) ->
      let fp = find_p_kid_else_error kids in
      let fpo = find_p_kid kids in
      let sir_id = fp "spotInstanceRequestId" in
      let spot_price = float_of_string (fp "spotPrice") in
      let state = spot_instance_request_state_of_string (fp "state") in
      let sir_type = spot_instance_request_type_of_string (fp "type") in
      let instance_id_opt = fpo "instanceId" in

      let launch_specification_x = find_kids_else_error kids "launchSpecification" in
      let fpo = find_p_kid launch_specification_x in
      let image_id_opt = fpo "imageId" in
      let key_name_opt = fpo "keyName" in
      let groups_x = find_kids_else_error launch_specification_x "groupSet" in
      let groups = List.map group_of_xml groups_x in
      let placement_x_opt = find_kids launch_specification_x "placement" in
      let placement_availability_zone_opt, placement_group_opt =
        match placement_x_opt with
          | None -> None, None
          | Some placement_x ->
              let availability_zone, placement_group_opt = placement_of_xml placement_x in
              Some availability_zone, placement_group_opt
      in

      (object
         method id = sir_id
         method spot_price = spot_price
         method state = state
         method sir_type = sir_type
         method instance_id_opt = instance_id_opt
         method image_id_opt = image_id_opt
         method key_name_opt = key_name_opt
         method groups = groups
         method placement_group_opt = placement_group_opt
       end)
  | _ ->
      raise (Error "RequestSpotInstancesResponse.spotInstanceRequestSet.item")

let request_spot_instances_of_xml = function
  | X.E ("RequestSpotInstancesResponse",_,[ _; X.E("spotInstanceRequestSet",_,items) ]) ->
      List.map spot_instance_request_of_xml items
  | _ ->
      raise (Error ("RequestSpotInstancesResponse"))

let request_spot_instances ?region creds spot_instance_request =
  let args = spot_instance_request_args spot_instance_request in
  let request = signed_request creds ?region
    (("Action", "RequestSpotInstances") :: args)
  in
  try_lwt
    lwt header, body = HC.get request in
    let xml = X.xml_of_string body in
    let rsp = request_spot_instances_of_xml xml in
    return (`Ok rsp)
  with
    | HC.Http_error (_,_,body) ->
      return (error_msg body)

(* describe spot instance requests *)
let describe_spot_instance_requests_of_xml = function
  | X.E("DescribeSpotInstanceRequestsResponse",_,[
    _;
    X.E("spotInstanceRequestSet",_,items)
  ]) ->
    List.map spot_instance_request_of_xml items

  | _ ->
    raise (Error "DescribeSpotInstanceRequestsResponse")

let sir_args_of_ids sir_ids =
  Util.list_map_i (
    fun i sir_id ->
      sprint "SpotInstanceRequestId.%d" (i+1), sir_id
  ) sir_ids

let describe_spot_instance_requests ?region creds sir_ids =
  let sir_ids_args = sir_args_of_ids sir_ids in
  let request = signed_request creds ?region
    (("Action", "DescribeSpotInstanceRequests") :: sir_ids_args) in
  try_lwt
    lwt header, body = HC.get request in
    let xml = X.xml_of_string body in
    return (`Ok (describe_spot_instance_requests_of_xml xml))
  with
    | HC.Http_error (_,_,body) ->
      return (error_msg body)

(* cancel spot instance requests *)
let item_of_xml = function
  | X.E("item",_,[
    X.E("spotInstanceRequestId",_,[X.P sir_id]);
    X.E("state",_,[X.P state_s])
  ]) ->
    sir_id, spot_instance_request_state_of_string state_s
  | _ -> raise (Error ("CancelSpotInstanceRequestsResponse.item"))

let cancel_spot_instance_requests_of_xml = function
  | X.E("CancelSpotInstanceRequestsResponse",_,[
    _; X.E("spotInstanceRequestSet",_,items_x)
  ])->
    List.map item_of_xml items_x
  | _ -> raise (Error "CancelSpotInstanceRequestsResponse")

let cancel_spot_instance_requests ?region creds sir_ids =
  let sir_ids_args = sir_args_of_ids sir_ids in
  let args = ("Action","CancelSpotInstanceRequests") :: sir_ids_args in
  let request = signed_request ?region creds args in
  try_lwt
    lwt header, body = HC.get request in
    let xml = X.xml_of_string body in
    return (`Ok (cancel_spot_instance_requests_of_xml xml))
  with
    | HC.Http_error (_,_,body) -> return (error_msg body)


let tag_set_item_of_xml = function
  | X.E("item", _, kids ) -> (
      match kids with
        | [X.E("resourceId", _, [X.P resource_id]);
           X.E("resourceType", _, [X.P resource_type]);
           X.E("key", _, [X.P key]);
           X.E("value", _, value_opt );
          ] -> (
            let value_opt =
              match value_opt with
                | [] -> None
                | [X.P value] -> Some value
                | _ -> raise (Error "tagSet.item:1")
            in
            object
              method resource_id = resource_id
              method resource_type = resource_type
              method key = key
              method value_opt = value_opt
            end
          )
        | _ ->
            raise (Error "tagSet.item:2")
    )
  | _ -> raise (Error "tagSet.item:3")

let describe_tags_response_of_xml = function
  | X.E("DescribeTagsResponse", _, kids ) -> (
      match find_kids kids "tagSet" with
        | Some tag_set_items  -> List.map tag_set_item_of_xml tag_set_items
        | None -> raise (Error "DescribeTagsResponse:1")
    )
  | _ -> raise (Error "DescribeTagsResponse:2")

let describe_tags creds = (* TODO filters *)
  let args = ["Action","DescribeTags"] in
  let request = signed_request creds args in
  try_lwt
    lwt header, body = HC.get request in
    let xml = X.xml_of_string body in
    let items = describe_tags_response_of_xml xml in
    return (`Ok items)
  with
    | HC.Http_error (_,_,body) -> return (error_msg body)


let create_tags creds tags =
  let args = ["Action", "CreateTags"] in
  let _, args = List.fold_left (
    fun (count, accu) tag ->
      let resource = sprint "ResourceId.%d" count, tag#resource_id in
      let key = sprint "Tag.%d.Key" count, tag#key in
      let value = sprint "Tag.%d.Value" count,
        (match tag#value_opt with
          | None -> ""
          | Some value -> value
        ) in
      count+1, resource :: key :: value :: accu
  ) (0, args) tags in
  let request = signed_request creds args in
  try_lwt
    lwt header, body = HC.get request in
    return `Ok
  with
    | HC.Http_error (_,_,body) -> return (error_msg body)

let delete_tags creds tags =
  let args = ["Action", "DeleteTags"] in
  let _,  args = List.fold_left (
    fun (count, accu) tag ->
      let resource = sprint "ResourceId.%d" count, tag#resource_id in
      let key = sprint "Tag.%d.Key" count, tag#key in
      let accu' =
        if tag#empty_value then
          let value = sprint "Tag.%d.Value" count, "" in
          value :: resource :: key :: accu
        else
          resource :: key :: accu
      in
      count+1, accu'
  ) (0, args) tags in
  let request = signed_request creds args in
  try_lwt
    lwt header, body = HC.get request in
    return `Ok
  with
    | HC.Http_error (_,_,body) -> return (error_msg body)

end
