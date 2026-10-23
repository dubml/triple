# Dubbo Triple

This repository provides the Triple protocol used by the Dubbo control plane. It supports HTTP/2-based Protobuf RPC calls, as well as HTTP unary calls using JSON or Protobuf.

The code in this repository covers client connection management, server-side protocol dispatch, message encoding and decoding, and metadata handling. It also supports unary and streaming RPCs, timeout control, call cancellation, TLS/mTLS, and graceful shutdown.