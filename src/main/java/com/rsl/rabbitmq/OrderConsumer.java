package com.rsl.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class OrderConsumer {
    public enum Service {
        INVENTORY,
        ANALYTICS,
        ERROR_LOGGER
    }

    private final String queueName;
    private final Service service;
    private final Set<String> processedOrderIds = ConcurrentHashMap.newKeySet();
    private Channel channel;

    public OrderConsumer(String queueName, Service service) {
        this.queueName = queueName;
        this.service = service;
    }

    public void start(Connection connection) throws IOException {
        channel = connection.createChannel();
        channel.basicQos(1);

        DeliverCallback callback = (consumerTag, delivery) -> handleMessage(
                delivery.getBody(), delivery.getEnvelope().getDeliveryTag());
        channel.basicConsume(queueName, false, callback, consumerTag -> { });
        System.out.println(service + " service is listening on " + queueName);
    }

    public void stop() throws IOException, TimeoutException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    private void handleMessage(byte[] body, long deliveryTag) throws IOException {
        Order order;
        try {
            order = Order.fromMessage(body);
        } catch (IllegalArgumentException exception) {
            System.err.println(service + " service received an invalid message: "
                    + exception.getMessage());
            channel.basicReject(deliveryTag, false);
            return;
        }

        if (!processedOrderIds.add(order.getOrderId())) {
            System.out.println(service + " service skipped duplicate order " + order.getOrderId());
            channel.basicAck(deliveryTag, false);
            return;
        }

        if (service == Service.INVENTORY && order.getQuantity() < 0) {
            System.out.println("Inventory Service: Rejecting invalid order " + order.getOrderId());
            channel.basicReject(deliveryTag, false);
            return;
        }

        if (service == Service.INVENTORY) {
            System.out.println("Inventory Service: Reserved inventory for order "
                    + order.getOrderId());
        } else if (service == Service.ANALYTICS) {
            System.out.println("Analytics Service: Recording high-value electronics sale for order "
                    + order.getOrderId());
        } else {
            System.out.println("Error Logger: Invalid order " + order.getOrderId()
                    + " routed to DLX");
        }

        channel.basicAck(deliveryTag, false);
    }
}
