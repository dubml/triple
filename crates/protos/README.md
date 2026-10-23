# Shared test and example protos

`proto/` is the single source of Echo and Greeter IDL used by Rust tests,
examples, and the Java Dubbo interoperability fixture. These are test contracts;
they do not define the Triple wire protocol.

`build.rs` generates Protobuf messages and tonic client/server bindings in
Cargo's `OUT_DIR`. Rust consumers use `protos::echo` and `protos::greeter`.
Protocol implementation crates reference these fixtures only through development
dependencies. `rpc-interop` owns the tonic test service and interoperability
assertions.

The Java fixture's Maven configuration reads the same `proto/` directory.
Keep package names, field numbers, and method paths consistent across languages.
`Greeter/Missing` is declared for unknown-method tests and remains unregistered
in example servers.

New contracts shared by tests or examples belong here. Private test IDL belongs
under its owning crate's `tests/proto/`. Generate bindings during builds rather
than checking generated files into the repository or copying IDL into consumers.
