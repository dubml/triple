# Dubbo Interoperability Test

```sh
TRIPLE_LISTEN=0.0.0.0:50051 TRIPLE_TRACE=1 cargo run -p triple-server --example server > /tmp/triple-dubbo-interop.log 2>&1
```

In another terminal, run the client from the repository root:

```sh
cd tests/dubbo-interop
mvn -B -ntp compile
mvn -B -ntp exec:java \
  -Dexec.mainClass=interop.ClientMain \
  -Dinterop.trace.log=/tmp/triple-dubbo-interop.log \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=warn
```

This client only verifies HTTP/2 Protobuf RPC interoperability; it does not cover HTTP/1.1 JSON calls.