(* SDB API *)
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

  let signed_request
      ?region
      ?(http_method=`POST)
      ?(http_uri="/")
      ?expires_minutes
      ?(safe=false)
      creds
      params =

    let http_host =
      match region with
        | Some r -> sprint "sdb.%s.amazonaws.com" r
        | None -> "sdb.amazonaws.com"
  in

    let params =
      ("Version", "2009-04-15" ) ::
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
      let key_equals_value = Util.encode_key_equals_value ~safe sorted_params in
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
    (http_host ^ http_uri), params


  (* XML readers *)

  let error_msg code' body =
    match X.xml_of_string body with
      | X.E ("Response",_, (
               X.E ("Errors",_, [
                      X.E ("Error",_,[
                             X.E ("Code",_,[X.P code]);
                             X.E ("Message",_,[X.P message]);
                             _
                           ]
                          )
                    ]
                   )
             ) :: _ ) -> `Error (code, message)
      | _ -> `Error ("unknown", body)


  let b64dec_if encoded s =
    if encoded then
      Util.base64_decoder s
    else
      s

  let b64enc_if encode s =
    if encode then
      Util.base64 s
    else
      s

  let domain_or_next_of_xml = function
    | X.E ("DomainName", _, [ X.P domain_name ]) -> `D domain_name
    | X.E ("NextToken", _, [ X.P next_token ]) -> `N next_token
    | _ -> raise (Error "ListDomainsResult.domain")

  let list_domains_response_of_xml = function
    | X.E ("ListDomainsResponse", _, [
             X.E ("ListDomainsResult", _, domain_or_next_list );
             _ ]) ->
        let domain_names, next_tokens = List.fold_left (
          fun (domain_names, next_tokens) domain_or_next_xml ->
            match domain_or_next_of_xml domain_or_next_xml with
              | `D domain_name -> domain_name :: domain_names, next_tokens
              | `N next_token -> domain_names, next_token :: next_tokens
        ) ([],[]) domain_or_next_list in
        (* preseve the order of domains in response *)
        let domain_names = List.rev domain_names in
        domain_names,
        (* check that we have no more than one NextToken *)
        (match next_tokens with
           | [next_token] -> Some next_token
           | [] -> None
           | _ -> raise (Error "ListDomainsResponse: more than one NextToken")
        )
    | _ -> raise (Error "ListDomainsResult")

  let attributes_of_xml encoded = function
    | X.E ("Attribute", _, [
             X.E ("Name", _, [ X.P name ]);
             X.E ("Value", _, [ X.P value ]);
           ]) ->
        b64dec_if encoded name,  Some (b64dec_if encoded value)

    | X.E ("Attribute", _, [
             X.E ("Name", _, [ X.P name ]);
             X.E ("Value", _, [ ]);
           ]) ->
        b64dec_if encoded name, None

    | _ -> raise (Error "Attribute 1")

  let get_attributes_response_of_xml encoded = function
    | X.E ("GetAttributesResponse", _, [
             X.E ("GetAttributesResult", _, attributes);
             _;
           ]) -> List.map (attributes_of_xml encoded) attributes
    | _ -> raise (Error "GetAttributesResponse")


  let attrs_of_xml encoded = function
    | X.E ("Attribute", _ , children) ->
      ( match children with
        | [ X.E ("Name", _, [ X.P name ]) ;
            X.E ("Value", _, [ X.P value ]) ;
          ] ->  b64dec_if encoded name, Some (b64dec_if encoded value)
        | [ X.E ("Name", _, [ X.P name ]) ;
            X.E ("Value", _, [ ]) ;
          ] -> b64dec_if encoded name, None
        | l -> raise (Error (sprint "fat list %d" (List.length l)))
      )
    | _ -> raise (Error "Attribute 3")

  let rec item_of_xml encoded acc token = function
    | [] -> (acc, token)
    | X.E ("Item", _, (X.E ("Name", _, [ X.P name ]) :: attrs)) :: nxt ->
        let a = List.map (attrs_of_xml encoded) attrs in
        item_of_xml encoded (((b64dec_if encoded name), a) :: acc) token nxt
    | X.E ("NextToken", _, [ X.P next_token ]) :: _ -> acc, (Some next_token)
    | _ -> raise (Error "Item")

  let select_of_xml encoded = function
    | X.E ("SelectResponse", _, [
             X.E ("SelectResult", _, items);
             _ ;
           ]) -> item_of_xml encoded [] None items
    | _ -> raise (Error "SelectResponse")

  (* list all domains *)

  let list_domains creds ?token () =
    let url, params = signed_request creds
      (("Action", "ListDomains")
       :: match token with
           None -> []
         | Some t -> [ "NextToken", t ]) in

    try_lwt
       lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in
       let xml = X.xml_of_string body in
       return (`Ok (list_domains_response_of_xml xml))
    with HC.Http_error (code, _, body) ->  return (error_msg code body)

  (* create domain *)

  let create_domain creds name =
    let url, params = signed_request creds [
      "Action", "CreateDomain" ;
      "DomainName", name
    ] in

    try_lwt
      lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in
      return `Ok
    with HC.Http_error (code, _, body) ->
      return (error_msg code body)

  (* delete domain *)

  let delete_domain creds name =
    let url, params = signed_request creds [
      "Action", "DeleteDomain" ;
      "DomainName", name
    ] in

    try_lwt
       lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in
       return `Ok
    with HC.Http_error (code, _, body) ->
      return (error_msg code body)

  (* put attributes *)

  let put_attributes ?(replace=false) ?(encode=true) creds ~domain ~item attrs =
    let _, attrs' = List.fold_left (
      fun (i, acc) (name, value_opt) ->
        let value_s =
          match value_opt with
            | Some value -> b64enc_if encode value
            | None -> ""
        in
        let value_p = sprint "Attribute.%d.Value" i, value_s in
        let name_p = sprint "Attribute.%d.Name" i, b64enc_if encode name in
        let acc =
          name_p :: value_p :: (
            if replace then
              (sprint "Attribute.%d.Replace" i, "true") :: acc
            else
              acc
          ) in
        i+1, acc
    ) (1, []) attrs in
    let url, params = signed_request creds
      (("Action", "PutAttributes")
       :: ("DomainName", domain)
       :: ("ItemName", b64enc_if encode item)
       :: attrs') in
    try_lwt
      lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in

      return `Ok
    with HC.Http_error (code, _, body) -> return (error_msg code body)

  (* batch put attributes *)

  let batch_put_attributes ?(replace=false) ?(encode=true) creds domain items =
    let _, attrs' = List.fold_left
      (fun (i, acc) (item_name, attrs) ->
         let item_name_p = sprint "Item.%d.ItemName" i, b64enc_if encode item_name in
         let _, acc = List.fold_left (
           fun (j, acc) (name, value_opt) ->
             let name_p = sprint "Item.%d.Attribute.%d.Name" i j,
               b64enc_if encode name in
             let value_s =
               match value_opt with
                 | Some value -> b64enc_if encode value
                 | None -> "" in
             let value_p = sprint "Item.%d.Attribute.%d.Value" i j, value_s in
             let acc' = name_p :: value_p ::
               if replace then
                  (sprint "Item.%d.Attribute.%d.Replace" i j, "true") :: acc
                else
                  acc
             in
             j+1, acc'
         ) (1, item_name_p :: acc) attrs in
         i+1, acc
      ) (1, []) items in

    let url, params = signed_request creds
      (("Action", "BatchPutAttributes")
       :: ("DomainName", domain)
       :: attrs') in
    try_lwt
      lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in

      return `Ok
    with HC.Http_error (code, _, body) ->  return (error_msg code body)

  (* get attributes *)

  let get_attributes ?(encoded=true) creds ~domain ?attribute ~item () =
    let attribute_name_p =
      match attribute with
        | None -> []
        | Some attribute_name ->
            [ "AttributeName", (b64enc_if encoded attribute_name) ]
    in
    let url, params = signed_request creds (
      ("Action", "GetAttributes") ::
        ("DomainName", domain) ::
        ("ItemName", b64enc_if encoded item) ::
        attribute_name_p
    ) in
    try_lwt
      lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in

      let xml = X.xml_of_string body in
      return (`Ok (get_attributes_response_of_xml encoded xml))
    with HC.Http_error (code, _, body) ->  return (error_msg code body)

  (* delete attributes *)

  let delete_attributes ?(encode=true) creds ~domain ~item attrs =
    let _, attrs' = List.fold_left (
      fun (i, acc) (name, value) ->
        let name_p = sprint "Attribute.%d.Name" i, b64enc_if encode name in
        let value_p = sprint "Attribute.%d.Value" i, b64enc_if encode value in
        i+1, name_p :: value_p :: acc
    ) (0,[]) attrs in
    let url, params = signed_request creds
      (("Action", "DeleteAttributes")
       :: ("DomainName", domain)
       :: ("ItemName", b64enc_if encode item)
       :: attrs') in
    try_lwt
       lwt header, body = HC.post ~body:(`String (Util.encode_post_url params)) url in
       return `Ok
    with HC.Http_error (code, _, body) ->
      return (error_msg code body)

  (* select: TODO if [encode=true], encode references to values in the
     select [expression].  This might not be easy, as the [expression]
     will have to be deconstructed (parsed). Alternatively,
     [expression] is replaced with an expression type, which would
     make value substitutions easier. Neither would work for numerical
     constraints.  *)

  let select ?(consistent=false) ?(encoded=true) ?(token=None) creds expression =
    let url, params = signed_request ~safe:true creds
      (("Action", "Select")
       :: ("SelectExpression", expression)
       :: ("ConsistentRead", sprint "%B" consistent)
       :: (match token with
         | None -> []
         | Some t -> [ "NextToken", t ])) in
    try_lwt
       let key_equals_value = Util.encode_key_equals_value ~safe:true params in
       let uri_query_component = String.concat "&" key_equals_value in
       lwt header, body = HC.post ~body:(`String uri_query_component) url in

       let xml = X.xml_of_string body in
       return (`Ok (select_of_xml encoded xml))
    with HC.Http_error (code, _, body) -> return (error_msg code body)

  (* select all records where attribute [name] equals [value] *)
  let select_where_attribute_equals ?(consistent=false) ?(encoded=true) ?(token=None) creds
      ~domain ~name ~value =
    let expression = sprint "select * from `%s` where `%s` = %S"
      domain (b64enc_if encoded name) (b64enc_if encoded value) in
    select ~consistent ~encoded ~token creds expression

end
