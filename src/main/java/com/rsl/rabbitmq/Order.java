package com.rsl.rabbitmq;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Order {
    private static final Pattern CATEGORY_PATTERN =
            Pattern.compile("\"category\"\\s*:\\s*\"([A-Za-z][A-Za-z0-9_-]*)\"");
    private static final Pattern QUANTITY_PATTERN =
            Pattern.compile("\"quantity\"\\s*:\\s*(-?\\d+)");

    private final String orderId;
    private final String category;
    private final int quantity;

    public Order(String orderId, String category, int quantity) {
        this.orderId = orderId;
        this.category = category;
        this.quantity = quantity;
    }

    public static Order fromRequest(String json) {
        String category = findValue(CATEGORY_PATTERN, json, "category").toLowerCase();
        int quantity;
        try {
            quantity = Integer.parseInt(findValue(QUANTITY_PATTERN, json, "quantity"));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("quantity must be a whole number");
        }

        return new Order(UUID.randomUUID().toString(), category, quantity);
    }

    public static Order fromMessage(byte[] body) {
        String json = new String(body, StandardCharsets.UTF_8);
        Matcher idMatcher = Pattern.compile("\"order_id\"\\s*:\\s*\"([^\"]+)\"").matcher(json);
        Matcher categoryMatcher = CATEGORY_PATTERN.matcher(json);
        Matcher quantityMatcher = QUANTITY_PATTERN.matcher(json);

        if (!idMatcher.find() || !categoryMatcher.find() || !quantityMatcher.find()) {
            throw new IllegalArgumentException("message is missing required order fields");
        }

        try {
            return new Order(
                    idMatcher.group(1),
                    categoryMatcher.group(1),
                    Integer.parseInt(quantityMatcher.group(1)));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("quantity must be a whole number");
        }
    }

    public String toJson() {
        return "{\"order_id\":\"" + orderId + "\",\"category\":\"" + category
                + "\",\"quantity\":" + quantity + "}";
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCategory() {
        return category;
    }

    public int getQuantity() {
        return quantity;
    }

    private static String findValue(Pattern pattern, String json, String fieldName) {
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            throw new IllegalArgumentException("request must include " + fieldName);
        }
        return matcher.group(1);
    }
}
