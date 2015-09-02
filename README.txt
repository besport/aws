(* OASIS_START *)
(* DO NOT EDIT (digest: a802600121e3c4a3bb49d9476657ebc2) *)

aws - AWS client for Amazon Web Services
========================================

Package to provide OCaml client access to Amazon services : S3, EC2, SQS,
SDB, FPS, IAM, DynamoDb, SES.

Aws depends directly on the following packages:

 - netstring
 - cryptokit
 - calendar
 - lwt
 - xmlm
 - yojson

It also depends on one of those packages:

  - [cohttp fork] (http://github.com/williamleferrand/ocaml-cohttp) (--enable-cohttp)

  - ocsigen (--enable-ocsigen) (default)

the cohttp driver does not support ssl for now. Some services (e.g. Ses may
no be available when using cohttp)

All of the packages except the [cohttp fork] are available via OPAM.

See the file [INSTALL.txt](INSTALL.txt) for building and installation
instructions.

[Home page](https://github.com/besport/aws/)

Copyright and license
---------------------

aws is distributed under the terms of the MIT License.

(* OASIS_STOP *)
