# RabbitMQ Order Processing

A small Java application that publishes orders to RabbitMQ and processes them with inventory, analytics, and error-logging consumers.

## Requirements

- Java 17 or newer
- Maven
- Docker

## 1. Start RabbitMQ

Run RabbitMQ with its management page enabled:

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

RabbitMQ's management page is available at [http://localhost:15672](http://localhost:15672). The default username and password are both `guest`.

## 2. Run the application

From the project directory, run:

```bash
mvn compile exec:java
```

The API listens on port `8080`. It connects to RabbitMQ at `localhost:5672` using the default `guest` credentials.

Optional environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `RABBITMQ_HOST` | `localhost` | RabbitMQ host |
| `RABBITMQ_PORT` | `5672` | RabbitMQ port |
| `RABBITMQ_USERNAME` | `guest` | RabbitMQ username |
| `RABBITMQ_PASSWORD` | `guest` | RabbitMQ password |
| `API_PORT` | `8080` | HTTP API port |

## 3. Place an order

Send a POST request to the API:

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"category":"electronics","quantity":2}'
```

The response contains the generated order ID. For example:

```json
{"message":"Order placed successfully","order_id":"generated-order-id"}
```

Try a clothing order to see that it is processed by inventory but not analytics:

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"category":"clothing","quantity":1}'
```

Try a negative quantity to see inventory reject the order and the error logger receive it through the dead-letter exchange:

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"category":"electronics","quantity":-1}'
```

Consumer messages are printed in the application terminal. Stop the application with `Ctrl+C`.

## How message routing works

- The API publishes orders to the `orders_exchange` topic exchange with routing keys such as `order.created.electronics`.
- The inventory queue is bound with `order.created.*` and receives orders in every category.
- The analytics queue is bound with `order.created.electronics` and receives only electronics orders.
- Inventory rejects orders with a negative quantity without requeuing them. RabbitMQ routes rejected messages through the `orders_dlx` dead-letter exchange to the error logger queue.
- Consumers use manual acknowledgements, a prefetch count of `1`, and an in-memory set of order IDs to skip duplicates while the application is running.
