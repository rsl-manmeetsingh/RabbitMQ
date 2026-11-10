package com.rsl.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class OrderApiServer {
    private static final String EXCHANGE_NAME = "orders_exchange";
    private static final int MAX_REQUEST_SIZE = 64 * 1024;

    private final Connection rabbitConnection;
    private final HttpServer server;
    private final ExecutorService executor;

    public OrderApiServer(Connection rabbitConnection, int port) throws IOException {
        this.rabbitConnection = rabbitConnection;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.executor = Executors.newFixedThreadPool(4);
        server.createContext("/api/orders", this::handleOrderRequest);
        server.setExecutor(executor);
    }

    public void start() {
        server.start();
        System.out.println("Order API is listening on port " + server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
        executor.shutdown();
    }

    private void handleOrderRequest(HttpExchange exchange) throws IOException {
        try {
            if (!"/api/orders".equals(exchange.getRequestURI().getPath())) {
                sendResponse(exchange, 404, "{\"error\":\"Endpoint not found\"}");
                return;
            }

            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Use POST for this endpoint\"}");
                return;
            }

            byte[] requestBody = exchange.getRequestBody().readNBytes(MAX_REQUEST_SIZE + 1);
            if (requestBody.length > MAX_REQUEST_SIZE) {
                sendResponse(exchange, 413, "{\"error\":\"Request body is too large\"}");
                return;
            }

            Order order;
            try {
                order = Order.fromRequest(new String(requestBody, StandardCharsets.UTF_8));
            } catch (IllegalArgumentException exception) {
                sendResponse(exchange, 400, "{\"error\":\"" + exception.getMessage() + "\"}");
                return;
            }

            String routingKey = "order.created." + order.getCategory();
            Map<String, Object> headers = Map.of("order_id", order.getOrderId());
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .messageId(order.getOrderId())
                    .headers(headers)
                    .contentType("application/json")
                    .deliveryMode(2)
                    .build();

            try (Channel channel = rabbitConnection.createChannel()) {
                channel.basicPublish(
                        EXCHANGE_NAME,
                        routingKey,
                        properties,
                        order.toJson().getBytes(StandardCharsets.UTF_8));
            }

            sendResponse(exchange, 202,
                    "{\"message\":\"Order placed successfully\",\"order_id\":\""
                            + order.getOrderId() + "\"}");
        } catch (IOException | TimeoutException exception) {
            System.err.println("Could not process order request: " + exception.getMessage());
            sendResponse(exchange, 500, "{\"error\":\"Could not publish the order\"}");
        } finally {
            exchange.close();
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseBody)
            throws IOException {
        byte[] response = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(statusCode, response.length);
        exchange.getResponseBody().write(response);
    }
}
