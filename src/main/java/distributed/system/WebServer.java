package distributed.system;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;

public class WebServer {
    private static final String TASK_ENDPOINT = "/task";
    private static final String STATUS_ENDPOINT = "/status";
    private final int port;
    private HttpServer server;

    public WebServer(int port) {
        this.port = port;
    }

    public static void main(String[] args) {
        int serverPort = 8080;
        if (args.length == 1) {
            serverPort = Integer.parseInt(args[0]);
        }
        WebServer webServer = new WebServer(serverPort);
        webServer.startServer();
        System.out.println("Server is listening at port = " + serverPort);
    }

    public void startServer() {
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        HttpContext statusContext = server.createContext(STATUS_ENDPOINT);
        HttpContext taskContext = server.createContext(TASK_ENDPOINT);
        statusContext.setHandler(this::handleStatusCheckRequest);
        taskContext.setHandler(this::handleTaskRequest);

        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
    }

    private void handleStatusCheckRequest(final HttpExchange exchange) {
        if (!exchange.getRequestMethod().equalsIgnoreCase("get")) {
            exchange.close();
            return;
        }
        String response = "Server is alive";
        sendResponse(response.getBytes(), exchange);
    }

    private void handleTaskRequest(final HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("post")) {
            exchange.close();
            return;
        }
        final var headers = exchange.getRequestHeaders();
        if (headers.containsKey("X-Test")
                && headers.get("X-Test").get(0).equalsIgnoreCase("true")) {
            final var response = "Test passed 123\n";
            sendResponse(response.getBytes(), exchange);
            return;
        }
        boolean debugMode = headers.containsKey("X-Debug")
                && headers.get("X-Debug").get(0).equalsIgnoreCase("true");
        final var startTime = System.nanoTime();
        final var requestBytes = exchange.getRequestBody().readAllBytes();
        final var responseBytes = calculateResponse(requestBytes);
        final var finishTime = System.nanoTime();
        if (debugMode) {
            String debugMessage = String.format("Operation took %d ns", finishTime - startTime);
            final var responseHeaders = exchange.getResponseHeaders();
            try {
                responseHeaders.put("X-Debug-Info", List.of(debugMessage));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        sendResponse(responseBytes, exchange);
    }

    private byte[] calculateResponse(final byte[] requestBytes) {
        String body = new String(requestBytes);
        final var numbers = body.split(",");
        BigInteger result = BigInteger.ONE;
        try {
            for (String number : numbers) {
                result = result.multiply(new BigInteger(number));
            }
        } catch (NumberFormatException e) {

            e.printStackTrace();
            throw new RuntimeException("Please provide only numbers", e);
        }
        return String.format("Result of multiplication is %s\n", result).getBytes();
    }

    private void sendResponse(final byte[] bytes, final HttpExchange exchange) {
        try {
            exchange.sendResponseHeaders(200, bytes.length);
            final var outputStream = exchange.getResponseBody();
            outputStream.write(bytes);
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            exchange.close();
        }
    }
}
