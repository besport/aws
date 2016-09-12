type node =
  | E of string * attr list * node list
  | P of string
and attr = (string * string) * string

exception ParseError

let xml_of_string s = try
  (* we drop the namespace part of the element here *)
  let el ((ns, name), atts) kids = E (name, atts, kids) in
  let data d = P d in
  let input = Xmlm.make_input ~strip:true (`String (0,s)) in
  let _, node = Xmlm.input_doc_tree ~el ~data input in
  node
with _ -> raise ParseError

let frag = function
  | E (name, attrs, kids) -> `El ((("", name), attrs), kids)
  | P d -> `Data d

let string_of_xml x =
  let buf = Buffer.create 100 in
  let output = Xmlm.make_output (`Buffer buf) in
  Xmlm.output_doc_tree frag output (None, x);
  Buffer.contents buf

let rec fetch_nodes xml l =
  match l with
    | [] -> xml
    | h::t ->
      match xml with
      | E (h_, _, xml)::_ when h = h_ -> fetch_nodes xml t
      | _::xml -> fetch_nodes xml l
      | _ -> raise Not_found

let nodes_of_string s xml =
  let l = Str.split (Str.regexp "\\.") s in
  fetch_nodes [ xml ] l

let data_of_string s xml =
  let l = Str.split (Str.regexp "\\.")  s in
  match fetch_nodes xml l with
    | [ P d ] -> d
    | _ -> raise Not_found
