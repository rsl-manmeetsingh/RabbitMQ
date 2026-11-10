package com.rsl.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class OrderApplication {
    private static final String ORDERS_EXCHANGE = "orders_exchange";
    private static final String DEAD_LETTER_EXCHANGE = "orders_dlx";
    private static final String INVENTORY_QUEUE = "inventory_orders_queue";
    private static final String ANALYTICS_QUEUE = "analytics_electronics_queue";
    private static final String DEAD_LETTER_QUEUE = "orders_dead_letter_queue";
    private static final String DEAD_LETTER_ROUTING_KEY = "order.invalid";

    private final Connection connection;
    private final OrderApiServer apiServer;
    private final OrderConsumer inventoryConsumer;
    private final OrderConsumer analyticsConsumer;
    private final OrderConsumer errorLoggerConsumer;

    private OrderApplication(Connection connection, int apiPort) throws Exception {
        this.connection = connection;
        declareQueues(connection);

        inventoryConsumer = new OrderConsumer(INVENTORY_QUEUE, OrderConsumer.Service.INVENTORY);
        analyticsConsumer = new OrderConsumer(ANALYTICS_QUEUE, OrderConsumer.Service.ANALYTICS);
        errorLoggerConsumer = new OrderConsumer(DEAD_LETTER_QUEUE, OrderConsumer.Service.ERROR_LOGGER);
        apiServer = new OrderApiServer(connection, apiPort);
    }

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(System.getenv().getOrDefault("RABBITMQ_HOST", "localhost"));
        factory.setPort(readIntegerSetting("RABBITMQ_PORT", 5672));
        factory.setUsername(System.getenv().getOrDefault("RABBITMQ_USERNAME", "guest"));
        factory.setPassword(System.getenv().getOrDefault("RABBITMQ_PASSWORD", "guest"));

        Connection connection = factory.newConnection("order-processing-application");
        OrderApplication application;
        try {
            application = new OrderApplication(
                    connection, readIntegerSetting("API_PORT", 8080));
            application.start();
        } catch (Exception exception) {
            connection.close();
            throw exception;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(application::stop));
    }

    private static void declareQueues(Connection connection) throws IOException, TimeoutException {
        try (Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(ORDERS_EXCHANGE, BuiltinExchangeType.TOPIC, true);
            channel.exchangeDeclare(DEAD_LETTER_EXCHANGE, BuiltinExchangeType.DIRECT, true);

            Map<String, Object> inventoryQueueArguments = new HashMap<>();
            inventoryQueueArguments.put("x-dead-letter-exchange", DEAD_LETTER_EXCHANGE);
            inventoryQueueArguments.put("x-dead-letter-routing-key", DEAD_LETTER_ROUTING_KEY);

            channel.queueDeclare(INVENTORY_QUEUE, true, false, false, inventoryQueueArguments);
            channel.queueBind(INVENTORY_QUEUE, ORDERS_EXCHANGE, "order.created.*");

            channel.queueDeclare(ANALYTICS_QUEUE, true, false, false, null);
            channel.queueBind(ANALYTICS_QUEUE, ORDERS_EXCHANGE, "order.created.electronics");

            channel.queueDeclare(DEAD_LETTER_QUEUE, true, false, false, null);
            channel.queueBind(
                    DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, DEAD_LETTER_ROUTING_KEY);
        }
    }

    private void start() throws IOException {
        inventoryConsumer.start(connection);
        analyticsConsumer.start(connection);
        errorLoggerConsumer.start(connection);
        apiServer.start();
    }

    private void stop() {
        apiServer.stop();
        closeConsumer(inventoryConsumer);
        closeConsumer(analyticsConsumer);
        closeConsumer(errorLoggerConsumer);
        try {
            connection.close();
        } catch (IOException exception) {
            System.err.println("Could not close RabbitMQ connection: " + exception.getMessage());
        }
    }

    private static void closeConsumer(OrderConsumer consumer) {
        try {
            consumer.stop();
        } catch (IOException | TimeoutException exception) {
            System.err.println("Could not stop consumer: " + exception.getMessage());
        }
    }

    private static int readIntegerSetting(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " must be a whole number", exception);
        }
    }
}
