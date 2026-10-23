# Dubbo 3.2.19 Triple Client 互通观察记录

核对日期：2026-10-14。固定 Dubbo/dubbo-compiler 3.2.19。
覆盖 HTTP/2 gRPC 兼容路径的 SayHello、Echo/Unary、Slow 线路期限、
Fail 四组对照与 Missing；观察到成功值、期限 trailers、错误状态映射及
rich status 描述缺失。HTTP/1.1 JSON 路径只有 curl 参与。
这是互通观察记录，不将官方客户端的 rich status 消息表现归为服务端缺陷。

## 夹具特殊性与直连

1. demo.Greeter/SayHello 同时注册在 RPC 与 HTTP 绑定。
   Protobuf HelloRequest.name=1、HelloReply.message=1；Missing 未注册。
2. fake/server.rs 读取 TRIPLE_LISTEN，默认 127.0.0.1:50051。
   0.0.0.0:50051 仅用于应对官方回环地址过滤。
   客户端仍用 setUrl("tri://127.0.0.1:50051")，过滤仍发生，实际连接 192.168.2.1。
   没有反射、修改官方库或本地代理。

[官方直连 API](https://dubbo.apache.org/en/overview/mannual/java-sdk/reference-manual/config/api/api/)
说明 setUrl 绕过注册中心。本轮 CONFIG connection_target=192.168.2.1:50051，
服务端 peer=192.168.2.1:52317；lsof 显示 TCP *:50051，nc 对目标地址成功。

## Slow：两个计时器的竞态

3.2.19 sources JAR 中，TripleInvoker.java:247-262 把同一个 timeout
写入 invocation、DeadlineFuture 和 createRequest；310-312 生成 timeout + "m"；
RequestMetadata.java:60 将它写为 grpc-timeout。
AbstractInvoker.java:289-292 用相同 invocation timeout 调用 AsyncRpcResult.get；
AsyncRpcResult.java:211-231 使用它计算本地等待期限。
TripleHeaderEnum.java:35、55-56、77-78 与 StreamUtils.java:97-98 排除
自定义 grpc-timeout attachment，未发现同步 Protobuf 调用的独立配置入口。
[官方超时说明](https://dubbo.apache.org/zh-cn/overview/mannual/java-sdk/tasks/framework/timeout/)
与 [Triple 规范](https://dubbo.apache.org/en/overview/reference/protocols/triple-spec/)
没有给出 3.2.19 分开设置两者的方式。

此前观察到服务端 grpc-status=4 被映射成 StatusRpcException(DEADLINE_EXCEEDED)。
本轮观察到本地 RpcException(code=2)，cause 为 TimeoutException，
消息含 Timeout after 300ms waiting for result.。两种结果时序依赖竞态，均接受。
本地超时不等于服务端状态映射；不能把服务端期限先到设为可控验证前提。
ClientMain 分别打印 server_status_mapping、local_timeout 或 unexpected。

本轮 Slow 请求头为 grpc-timeout: 300m，即 300 ms。
请求 timestamp_unix_ms=1791963480951；trailers 帧
timestamp_unix_ms=1791963481254，elapsed_us=302350（302.350 ms）。
判据 250–400 ms 满足。单调时钟起点是服务端读到 HEADERS，
终点是响应帧被 TCP poll_write 接受，不是客户端收包时间。

## Fail 二维对照

四个方法同为 PERMISSION_DENIED(7)，其余条件相同。
公开 triple_rpc::Status::new 默认 details 为空；不带 details 的两组直接用它构造。
带 details 的两组保持 with_details(&[8, 7])，线路 Base64 为 CAc。

| 方法 | 线路 grpc-status | HPACK 解码 grpc-message 原值 | grpc-status-details-bin | 客户端 StatusRpcException 状态与消息 | 原消息断言 | 裁定 |
|---|---|---|---|---|---|---|
| Fail | 7 | denied: %E6%B5%8B%E8%AF%95 %25 | CAc | PERMISSION_DENIED(7)，空字符串 | 未满足 | 采用 C，保留观察结果 |
| FailPlain | 7 | denied: plain | CAc | PERMISSION_DENIED(7)，空字符串 | 未满足 | 采用 C，保留观察结果 |
| FailNoDetails | 7 | denied: %E6%B5%8B%E8%AF%95 %25 | 缺省 | PERMISSION_DENIED(7)，denied: 测试 % | 满足 | 否 |
| FailNoDetailsPlain | 7 | denied: plain | 缺省 | PERMISSION_DENIED(7)，denied: plain | 满足 | 否 |

观察：移除 details 后消息出现；仅改成纯 ASCII，带 details 时仍为空。
FailNoDetails 的客户端消息显示 percent-decoding 已发生。

3.2.19 sources JAR 的直接依据：
- TripleClientStream.java:349-357 读取 grpc-message，调用 TriRpcStatus.decodeMessage。
- TripleClientStream.java:375-396 读取 grpc-status-details-bin，解析 google.rpc.Status，
  使用其 code 与 message 构造状态。
- TripleClientStream.java:242-253 优先使用上述 detailStatus，再调用 listener.onComplete。
- TriRpcStatus.java:147-152 调用 QueryStringDecoder.decodeComponent。

本次空描述来自官方客户端选用 code-only rich status 的 message。
原始描述中的 % 被再次解码时的表现，见下节 A/B/C 对照。

## 官方客户端 3.2.19 的 rich status 行为（不是服务端缺陷）

采用方案 C：保持 details 透传，不提交任何改写 details 的生产改动。
互通结论中 Fail 行官方客户端消息为空，属于观察到的行为，不属于服务端缺陷或修复成果。
示例 Fail/FailPlain 的 with_details(&[8, 7]) 保持原样；CAc 解码为 08 07，
只包含 code=7，message 缺省为空。格式及状态码一致性有效。

### 三方案实测对照

以下为 2026-10-14 的独立实验结果，Fail 描述为 denied: 测试 %，
FailPlain 描述为 denied: plain。三组外层 grpc-status 均为 7；
外层 grpc-message 分别为 denied: %E6%B5%8B%E8%AF%95 %25 与 denied: plain。
实验在 9587229 的透传实现上，由示例夹具直接构造 details 字节，
每组分别运行一次官方客户端与 tonic 客户端，实验夹具随后恢复。

| 方案 | 官方客户端 Fail 结果 | 官方客户端 FailPlain 结果 | tonic 解码出的 details.message（Fail / FailPlain） | 对 google.rpc.Status.message 语义的影响 | 所需生产改动 |
|---|---|---|---|---|---|
| A：message 写原始描述 | CANCELLED(1)，消息 null；异常 unterminated escape sequence | PERMISSION_DENIED(7)，denied: plain | denied: 测试 % / denied: plain | 保留原始普通字符串描述 | 编码时补写 message；已废弃的尝试还会经 with_details 改写接收端 details，不采用 |
| B：message 写 percent-encoded 描述 | PERMISSION_DENIED(7)，denied: 测试 % | PERMISSION_DENIED(7)，denied: plain | denied: %E6%B5%8B%E8%AF%95 %25 / denied: plain | Protobuf 字节合法，但描述成为编码串，增加无规范依据的编码约定 | 编码时补写并 percent-encode message，不采用 |
| C：仅 code，保持透传 | PERMISSION_DENIED(7)，空字符串 | PERMISSION_DENIED(7)，空字符串 | 空字符串 / 空字符串 | 空 message 合法，保留调用方提供的内容 | 无；with_details 校验后保留原始字节 |

A/B/C 的 Fail 线路 grpc-status-details-bin 依次为
CAcSEGRlbmllZDog5rWL6K+VICU、CAcSHmRlbmllZDogJUU2JUI1JThCJUU4JUFGJTk1ICUyNQ、CAc；
FailPlain 依次为 CAcSDWRlbmllZDogcGxhaW4、CAcSDWRlbmllZDogcGxhaW4、CAc。
A 的 Fail 出现 IllegalArgumentException：unterminated escape sequence at index 11 of: denied: 测试 %。
B/C 未出现该解码异常，但仍记录了调用返回业务错误时的 RpcExceptionFilter、AsyncRpcResult 日志。
tonic 三组的外层 message 均保持原始描述；表内 details.message 由测试用 prost 解码，
tonic 的 Status.message 来自外层 grpc-message，details 保持为独立的不透明字节。

### 源码与规范核对

Dubbo 3.2.19 sources JAR 的 SHA-256：

    0b4328447c899c4c447a025211c5009cec9954471c1d5e5d9335d3f2a99e6f52

JAR 内 org/apache/dubbo/rpc/protocol/tri/stream/TripleClientStream.java：
349-359 行读取外层 grpc-message，355 行调用 TriRpcStatus.decodeMessage；
386-396 行读取并解析 grpc-status-details-bin，395-396 行用 rich status 的 code 与 message
构造客户端状态，并再次调用 TriRpcStatus.decodeMessage(statusDetail.getMessage())。
242-253 行优先采用该 detailStatus，因此空 rich message 会覆盖非空外层描述。
org/apache/dubbo/rpc/TriRpcStatus.java:147-152 的同一个 decodeMessage 函数
对非空输入调用 QueryStringDecoder.decodeComponent；它对 A 的末尾裸 % 抛出异常。

[grpc-go 的 internal/status/status.go](https://github.com/grpc/grpc-go/blob/master/internal/status/status.go)
核对到 NewWithProto 直接反序列化 google.rpc.Status，code 相符时保留整个消息；
Message() 直接返回 s.s.Message，不对 details.message 做 percent-decoding。
该结论来自源码阅读，本次没有运行 grpc-go 客户端。

[tonic 0.14.6 的 status.rs:466](https://docs.rs/crate/tonic/0.14.6/source/src/status.rs#466)
的 from_header_map 只对外层 grpc-message 做 percent-decoding；
grpc-status-details-bin 经 Base64 解码后保留为不透明字节，不对其中的 message 做 percent-decoding。
核对来源是本地 tonic-0.14.6/src/status.rs:466-507；发布包的 .cargo_vcs_info.json
记录源码提交 6cb6056b5a748bc5a29bd48f4602dbc4e552bb7d，path_in_vcs 为 tonic。
上表另以真实 tonic 客户端接收后用 prost 解码 details 验证。

[gRPC HTTP/2 规范](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
规定 grpc-message 使用 percent-encoding，Protobuf 路径的 grpc-status-details-bin
是经 Base64 编码的 google.rpc.Status，其中 code 不得与 grpc-status 矛盾。
[google.rpc.Status 定义](https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto)
将 message 定义为普通 string。
[官方 Triple 规范](https://dubbo.apache.org/en/overview/reference/protocols/triple-spec/)
同样说明外层 grpc-message 的 percent-encoding。
关于 details.message 应当 percent-encoded 的说明：文档未给出。
规范建议开发者消息用英文，本夹具的非 ASCII 内容专用于编码往返观察。

### 裁定与调用方选择

官方 Dubbo 3.2.19 客户端在存在 grpc-status-details-bin 时用其中的 message 覆盖外层 grpc-message，
且对 details.message 再做 percent-decoding（TripleClientStream.java:395-396、TriRpcStatus.java:147-152）。
google.rpc.Status.message 是普通字符串，grpc-go 与 tonic 都不对它做 percent-decoding。
方案 A 对含 % 的描述让官方客户端得到 CANCELLED、消息 null，比旧行为更糟；
方案 B 让官方客户端得到正确消息，但 grpc-go、tonic 读取到编码串，且无规范依据；
方案 C 保持透传，官方客户端状态码正确、消息为空，其他客户端行为不变。
裁定：官方客户端 3.2.19 对 rich status 的 message 处理存在两处与规范不符的行为，
服务端不为此改变线路语义，采用 C。未提交任何改写 details 的生产改动。

需要官方客户端显示描述时，调用方可以不带 details：FailNoDetails 与 FailNoDetailsPlain
已验证分别显示 denied: 测试 % 与 denied: plain。
调用方也可以在 details 中写原始描述：FailPlain 已验证显示 denied: plain；
但含 % 的原始描述在 3.2.19 上会触发上述解码异常，不能保证显示。
二者都是调用方的选择，服务端不替调用方改写 details，也不统一转成方案 B 的编码串。

## 其他调用汇总

| 调用 | 预期 | 线路 | 客户端 | 结果 |
|---|---|---|---|---|
| SayHello | OK，Hello Alice | 0，空消息 | Hello Alice | 满足 |
| Echo/Unary | OK，baseline | 0，空消息 | baseline | 满足 |
| Slow 线路 | 4，250–400 ms | 4，RPC deadline exceeded，302.350 ms | 本地超时 | 线路判据满足 |
| Slow 客户端 | 服务端状态映射或本地超时 | 4，RPC deadline exceeded | 本轮 local_timeout；此前 server_status_mapping | 竞态判据满足 |
| Missing | UNIMPLEMENTED | 12，unknown method | StatusRpcException：UNIMPLEMENTED(12)，unknown method | 满足 |
| HTTP/1.1 JSON SayHello | HTTP 200，Hello Alice | HTTP 200，JSON 正文 | curl：Hello Alice | 仅 curl 观察 |

所有官方请求为 POST、application/grpc+proto、te=trailers，
:authority=127.0.0.1:50051。tri-consumer-appname=triple-interop 作为应用 metadata；
grpc-timeout 用于期限；grpc-accept-encoding=gzip,snappy 是保留头，
不协商响应压缩；timeout=300 或 1000 是应用 metadata，不用于期限。
未观察到其他 tri-* 或 grpc-* 请求头。

## 重现命令与退出码

仓库根目录，用用户主目录下的新建日志文件运行；以下命令用于复现原始互通观察：

    TRIPLE_LISTEN=0.0.0.0:50051 TRIPLE_TRACE=1 cargo run -p triple-server --example server > "$HOME/triple-4b-control-server.log" 2>&1
    lsof -nP -iTCP:50051 -sTCP:LISTEN
    nc -vz 192.168.2.1 50051
    curl -sS -i --http1.1 -H 'Content-Type: application/json' --data '[{"name":"Alice"}]' http://127.0.0.1:50051/demo.Greeter/SayHello
    cd crates/server/interop-dubbo
    mvn -B -ntp compile
    mvn -B -ntp exec:java -Dexec.mainClass=interop.ClientMain -Dinterop.trace.log="$HOME/triple-4b-control-server.log" -Dorg.slf4j.simpleLogger.defaultLogLevel=warn

ClientMain 每行独立执行，最后读取 trace，打印线路与客户端汇总。
ClientMain 的原消息断言保持原样，两组空消息仍打印 pending；这是既有程序的观察标记，
按本次裁定不代表服务端存在待修复缺陷。四组实验数据完整时记录程序退出 0；
线路数据缺失、实验未完成或其他调用判据失败仍退出非零。
本轮输出 record_complete=true experiments_complete=true pending=true，Maven 退出 0。

原互通记录的 fmt、touch 后的 clippy --workspace --all-targets -D warnings、cargo test --workspace、
mvn compile 均完成，未修改现有测试或生产库。TRIPLE_TRACE 只观察 I/O，
实际分流由 Server::serve_connection 完成。示例服务已停止。

本次仅更新 README 中的三方案观察及裁定；没有重新运行上述构建、测试或互通命令。
