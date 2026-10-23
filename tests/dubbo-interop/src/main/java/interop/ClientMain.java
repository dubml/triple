package interop;

import demo.Greeter;
import demo.HelloRequest;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.dubbo.rpc.RpcException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.rpc.StatusRpcException;
import org.apache.dubbo.rpc.TriRpcStatus.Code;
import test.Echo;
import test.EchoMessage;

public final class ClientMain {
    private record Row(String name, String expected, String actual, boolean pass, boolean pending) {}
    private static final List<Row> rows = new ArrayList<>();
    private static boolean experimentsComplete = true;
    public static void main(String[] args) throws Exception {
        dumpConfiguration(args);
        ReferenceConfig<Greeter> greeter = reference(Greeter.class, 1000);
        ReferenceConfig<Echo> echo = reference(Echo.class, 300);
        DubboBootstrap bootstrap = DubboBootstrap.getInstance()
            .application(new ApplicationConfig("triple-interop"))
            .registry(new RegistryConfig("N/A"))
            .reference(greeter).reference(echo).start();
        try {
            Greeter g = greeter.get();
            Echo e = echo.get();
            System.out.println("CONFIG Greeter.final_url=" + greeter.getInvoker().getUrl().toFullString());
            System.out.println("CONFIG Echo.final_url=" + echo.getInvoker().getUrl().toFullString());
            check("Greeter/SayHello", Code.OK, "Hello Alice", null, () -> g.sayHello(
                HelloRequest.newBuilder().setName("Alice").build()).getMessage());
            check("Echo/Unary", Code.OK, "baseline", null, () -> e.unary(
                EchoMessage.newBuilder().setText("baseline").build()).getText());
            checkSlowOutcome(() -> e.slow(
                EchoMessage.newBuilder().setText("slow").build()).getText());
            check("Echo/Fail", Code.PERMISSION_DENIED, null, "denied", () -> e.fail(
                EchoMessage.newBuilder().setText("fail").build()).getText());
            check("Echo/FailPlain", Code.PERMISSION_DENIED, null, "denied", () -> e.failPlain(
                EchoMessage.newBuilder().setText("fail").build()).getText());
            check("Echo/FailNoDetails", Code.PERMISSION_DENIED, null, "denied", () -> e.failNoDetails(
                EchoMessage.newBuilder().setText("fail").build()).getText());
            check("Echo/FailNoDetailsPlain", Code.PERMISSION_DENIED, null, "denied", () -> e.failNoDetailsPlain(
                EchoMessage.newBuilder().setText("fail").build()).getText());
            check("Greeter/Missing", Code.UNIMPLEMENTED, null, null, () -> g.missing(
                HelloRequest.newBuilder().setName("Alice").build()).getMessage());
            summarize();
            // Fail variants are observations: a message defect remains pending, but complete data may be committed.
            if (!experimentsComplete || rows.stream().anyMatch(row -> !row.pass() && !row.name().startsWith("Echo/Fail"))) {
                throw new AssertionError("interop record incomplete or non-experiment row failed");
            }
            System.out.println("INTEROP record_complete=true experiments_complete=true pending="
                + rows.stream().anyMatch(Row::pending));
        } finally {
            bootstrap.stop();
        }
    }
    private static <T> ReferenceConfig<T> reference(Class<T> type, int timeout) {
        ReferenceConfig<T> ref = new ReferenceConfig<>();
        ref.setInterface(type);
        // Official point-to-point API; no global consumer URL or address override.
        ref.setUrl("tri://127.0.0.1:50051");
        ref.setCheck(false); ref.setRetries(0); ref.setTimeout(timeout);
        System.out.println("CONFIG " + type.getName() + ".url=" + ref.getUrl()
            + " check=false retries=0 timeout=" + timeout);
        return ref;
    }
    private static void check(String name, Code expected, String valueExpected, String messagePart,
                              java.util.concurrent.Callable<String> action) {
        long start = System.nanoTime();
        try {
            String value = action.call();
            boolean pass = expected == Code.OK && valueExpected.equals(value);
            rows.add(new Row(name, expected.name(), "OK(0): " + value, pass, !pass && expected != Code.OK));
            System.out.println("INTEROP " + name + " expected=" + expected
                + " actual=OK status=0 message=" + value + " pass=" + pass);
        } catch (Exception error) {
            StatusRpcException status = statusCause(error);
            String actualCode = status == null ? "unknown" : status.getStatus().code.name();
            String actualNumber = status == null ? "unknown" : Integer.toString(status.getStatus().code.code);
            String message = status == null ? error.getMessage() : status.getStatus().description;
            boolean pass = expected != Code.OK && status != null && status.getStatus().code == expected
                && (messagePart == null || (message != null && message.contains(messagePart)));
            String actual = actualCode + "(" + actualNumber + "): " + message
                + "; type=" + error.getClass().getName()
                + "; status_type=" + (status == null ? "none" : status.getClass().getName());
            rows.add(new Row(name, expected.name(), actual, pass, !pass && expected != Code.OK));
            System.out.println("INTEROP " + name + " expected=" + expected + " actual=" + actualCode
                + " status=" + actualNumber + " message=" + message + " type=" + error.getClass().getName()
                + " status_type=" + (status == null ? "none" : status.getClass().getName())
                + " elapsed_ms=" + (System.nanoTime() - start) / 1_000_000 + " pass=" + pass);
            if (!pass && (!name.startsWith("Echo/Fail") || status == null)) error.printStackTrace(System.out);
        }
    }
    private static void checkSlowOutcome(java.util.concurrent.Callable<String> action) {
        long start = System.nanoTime();
        try {
            String value = action.call();
            rows.add(new Row("Echo/Slow client", "server DEADLINE_EXCEEDED mapping OR local TimeoutException (race)",
                "OK(0): " + value, false, false));
        } catch (Exception error) {
            TimeoutException timeout = null;
            for (Throwable cause = error; cause != null; cause = cause.getCause()) {
                if (cause instanceof TimeoutException) { timeout = (TimeoutException) cause; break; }
            }
            StatusRpcException status = statusCause(error);
            boolean localTimeout = error instanceof RpcException && status == null && timeout != null
                && "Timeout after 300ms waiting for result.".equals(timeout.getMessage());
            boolean serverStatus = status != null && status.getStatus().code == Code.DEADLINE_EXCEEDED
                && status.getStatus().description != null && status.getStatus().description.contains("deadline");
            boolean pass = localTimeout || serverStatus;
            String actual = "outcome=" + (serverStatus ? "server_status_mapping" : localTimeout ? "local_timeout" : "unexpected")
                + "; type=" + error.getClass().getName()
                + "; rpc_exception_code=" + (error instanceof RpcException ? ((RpcException) error).getCode() : "unknown")
                + "; grpc_status=" + (status == null ? "unknown" : status.getStatus().code.code)
                + "; status_message=" + (status == null ? "unknown" : status.getStatus().description)
                + "; cause=" + (error.getCause() == null ? "none" : error.getCause().getClass().getName())
                + "; timeout_cause=" + (timeout == null ? "none" : timeout.getClass().getName())
                + "; message=" + error.getMessage();
            rows.add(new Row("Echo/Slow client", "server DEADLINE_EXCEEDED mapping OR local TimeoutException (race)", actual, pass, false));
            System.out.println("INTEROP Echo/Slow client " + actual
                + " elapsed_ms=" + (System.nanoTime() - start) / 1_000_000 + " pass=" + pass);
            if (!pass) error.printStackTrace(System.out);
        }
    }
    private static void summarize() throws Exception {
        // Reads the observer log after all calls. Missing wire evidence fails the run.
        List<String> trace = Files.readAllLines(Path.of(System.getProperty("interop.trace.log")));
        List<Row> summary = new ArrayList<>();
        for (Row row : rows) {
            String path = row.name().startsWith("Greeter/") ? "/demo." + row.name()
                : row.name().startsWith("Echo/Slow") ? "/test.Echo/Slow" : "/test." + row.name();
            String request = trace.stream().filter(line -> line.contains(" path=" + path + " ")).findFirst().orElse("");
            Matcher key = Pattern.compile("connection=(\\d+) stream=(\\d+)").matcher(request);
            String trailers = "unknown";
            if (key.find()) {
                String prefix = "TRIPLE_TRACE response connection=" + key.group(1) + " stream=" + key.group(2) + " ";
                trailers = trace.stream().filter(line -> line.startsWith(prefix) && line.contains("grpc-status"))
                    .findFirst().orElse("unknown");
            }
            if (row.name().startsWith("Echo/Fail")) {
                boolean withDetails = !row.name().contains("NoDetails");
                String message = row.name().endsWith("Plain") ? "denied: plain" : "denied: %E6%B5%8B%E8%AF%95 %25";
                boolean complete = trailers.contains("(\"grpc-status\", \"7\")")
                    && trailers.contains("(\"grpc-message\", \"" + message + "\")")
                    && (withDetails ? trailers.contains("(\"grpc-status-details-bin\", \"CAc\")")
                        : !trailers.contains("grpc-status-details-bin"))
                    && row.actual().contains("status_type=org.apache.dubbo.rpc.StatusRpcException");
                experimentsComplete &= complete;
                System.out.println("EXPERIMENT " + row.name() + " wire=" + trailers
                    + " grpc_status_details_bin_absent=" + !trailers.contains("grpc-status-details-bin")
                    + " client=" + row.actual() + " data_complete=" + complete);
            }
            if (row.name().equals("Echo/Slow client")) {
                Matcher elapsed = Pattern.compile("elapsed_us=(\\d+)").matcher(trailers);
                boolean wirePass = request.contains("(\"grpc-timeout\", \"300m\")")
                    && trailers.contains("(\"grpc-status\", \"4\")") && elapsed.find()
                    && Long.parseLong(elapsed.group(1)) >= 250_000 && Long.parseLong(elapsed.group(1)) <= 400_000;
                summary.add(new Row("Echo/Slow wire", "grpc-timeout=300m; grpc-status=4; 250-400ms",
                    trailers, wirePass, false));
            }
            summary.add(new Row(row.name(), row.expected(), "wire=" + trailers + "; client=" + row.actual(),
                row.pass() && !trailers.equals("unknown"), row.pending()));
        }
        rows.clear(); rows.addAll(summary);
        System.out.println("SUMMARY call | expected | actual_wire_and_client | pass | pending");
        for (Row row : rows) System.out.println("SUMMARY " + row.name() + " | " + row.expected() + " | "
            + row.actual() + " | " + row.pass() + " | " + row.pending());
    }
    private static StatusRpcException statusCause(Throwable error) {
        for (int depth = 0; error != null && depth < 32; depth++, error = error.getCause()) {
            if (error instanceof StatusRpcException) return (StatusRpcException) error;
        }
        return null;
    }
    private static void dumpConfiguration(String[] args) throws Exception {
        System.out.println("CONFIG args=" + Arrays.toString(args));
        System.out.println("CONFIG startup=" + System.getProperty("sun.java.command"));
        System.out.println("CONFIG jvm_args=" + ManagementFactory.getRuntimeMXBean().getInputArguments());
        Properties properties = System.getProperties();
        long count = properties.stringPropertyNames().stream().filter(k -> k.startsWith("dubbo.")).count();
        System.out.println("CONFIG dubbo_system_property_count=" + count);
        properties.stringPropertyNames().stream().filter(k -> k.startsWith("dubbo.")).sorted()
            .forEach(k -> System.out.println("CONFIG system " + k + "=" + properties.getProperty(k)));
        for (String key : new String[] {"DUBBO_IP_TO_REGISTRY", "DUBBO_IP_TO_BIND"}) {
            System.out.println("CONFIG env " + key + "=" + System.getenv(key));
            System.out.println("CONFIG system " + key + "=" + System.getProperty(key));
        }
        Enumeration<URL> resources = ClientMain.class.getClassLoader().getResources("dubbo.properties");
        int files = 0;
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement(); files++;
            System.out.println("CONFIG dubbo.properties=" + resource);
            Properties config = new Properties();
            try (java.io.InputStream input = resource.openStream()) { config.load(input); }
            config.stringPropertyNames().stream().filter(k -> k.matches(".*(address|url|register|registry|network|host|port).*"))
                .sorted().forEach(k -> System.out.println("CONFIG file " + k + "=" + config.getProperty(k)));
        }
        System.out.println("CONFIG dubbo.properties.count=" + files);
        System.out.println("CONFIG application.name=triple-interop registry.address=N/A");
        System.out.println("CONFIG NetUtils.getLocalHost=" + NetUtils.getLocalHost());
        System.out.println("CONFIG filterLocalHost(127.0.0.1)=" + NetUtils.filterLocalHost("127.0.0.1"));
        System.out.println("CONFIG connection_target=" + NetUtils.filterLocalHost("127.0.0.1") + ":50051");
    }
}
