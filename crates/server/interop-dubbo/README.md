# Dubbo 3.2.19 Triple Client 互通观察记录

核对日期：2026-10-14。固定 Dubbo/dubbo-compiler 3.2.19。
覆盖 HTTP/2 gRPC 兼容路径的 SayHello、Echo/Unary、Slow 线路期限、
Fail 四组对照与 Missing；观察到成功值、期限 trailers、错误状态映射及
rich status 描述缺失。HTTP/1.1 JSON 路径只有 curl 参与。
这是互通记录提交，包含未修复缺陷，不表示全部行为满足业务期望。

## 夹具特殊性与直连

1. demo.Greeter/SayHello 同时注册在 RPC 与 HTTP 绑定。
   Protobuf HelloRequest.name=1、HelloReply.message=1；Missing 未注册。
2. examples/server.rs 读取 TRIPLE_LISTEN，默认 127.0.0.1:50051。
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
公开 rpc::Status::new 默认 details 为空；不带 details 的两组直接用它构造。
带 details 的两组保持 with_details(&[8, 7])，线路 Base64 为 CAc。

| 方法 | 线路 grpc-status | HPACK 解码 grpc-message 原值 | grpc-status-details-bin | 客户端 StatusRpcException 状态与消息 | 消息断言 | 待决定 |
|---|---|---|---|---|---|---|
| Fail | 7 | denied: %E6%B5%8B%E8%AF%95 %25 | CAc | PERMISSION_DENIED(7)，空字符串 | 未满足 | 修复待决定 |
| FailPlain | 7 | denied: plain | CAc | PERMISSION_DENIED(7)，空字符串 | 未满足 | 修复待决定 |
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

因此本次描述为空由带 details 的 code-only rich status 引起，
不是“客户端不做 percent-decoding”的观察结果。

## 发现的缺陷（未修复）

示例 Fail/FailPlain 的 rich status 只提供 code=7，message 缺省为空，
同时 grpc-message 提供非空描述。官方客户端优先使用 rich status，显示空消息。
RPC binding 接受并透传了这组描述不一致的状态。本提交保留此现场，不改生产库。
是否处理该描述缺失、如何约束 rich status 与外层消息的一致性，仍待用户裁定。

[gRPC HTTP/2 规范](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
规定 Protobuf 路径的 grpc-status-details-bin 内容是序列化 google.rpc.Status，
经 Base64 编码；其中 code 不得与 grpc-status 矛盾。
[google.rpc.Status 定义](https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto)
含 code=1、message=2、details=3；message 是普通字符串，不是 grpc-message 的编码值。
当前 CAc 解码为 08 07，只包含 code=7，格式及状态码一致性有效。
为了保留本夹具的描述，rich status 应同时填入 message="denied: 测试 %"
或 message="denied: plain"；这不是宣称规范强制非空 message。
规范建议开发者消息用英文，本夹具的非 ASCII 内容专用于编码往返观察。
未在此提交内修改 details 内容或任何生产 API。

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

仓库根目录，用新建日志文件运行：

    TRIPLE_LISTEN=0.0.0.0:50051 TRIPLE_TRACE=1 cargo run -p triple-server --example server > /tmp/triple-4b-control-server.log 2>&1
    lsof -nP -iTCP:50051 -sTCP:LISTEN
    nc -vz 192.168.2.1 50051
    curl -sS -i --http1.1 -H 'Content-Type: application/json' --data '[{"name":"Alice"}]' http://127.0.0.1:50051/demo.Greeter/SayHello
    cd crates/server/interop-dubbo
    mvn -B -ntp compile
    mvn -B -ntp exec:java -Dexec.mainClass=interop.ClientMain -Dinterop.trace.log=/tmp/triple-4b-control-server.log -Dorg.slf4j.simpleLogger.defaultLogLevel=warn

ClientMain 每行独立执行，最后读取 trace，打印线路与客户端汇总。
Fail 各行的消息断言仍保留：两组空消息会标记 pending，不伪装为满足。
本轮授权允许在四组实验数据完整时提交记录；因此实验完整但消息缺陷未修复时
退出 0。线路数据缺失、实验未完成或其他调用判据失败仍退出非零。
本轮输出 record_complete=true experiments_complete=true pending=true，Maven 退出 0。

fmt、touch 后的 clippy --workspace --all-targets -D warnings、cargo test --workspace、
mvn compile 均完成，未修改现有测试或生产库。TRIPLE_TRACE 只观察 I/O，
实际分流由 Server::serve_connection 完成。示例服务已停止。
