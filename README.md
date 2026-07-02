# i2i-Academy-EDAWithApacheKafka-6

# EDA with Apache Kafka

This project is a simple example of Event-Driven Architecture (EDA) using Apache Kafka.
The aim of the project is to show how a simple producer-consumer system works using Apache Kafka and Java.

## Technologies Used

- Java
- Apache Kafka
- Maven
- Docker

## How It Works

In this project, the producer sends sample order messages to a Kafka topic, and the consumer reads these messages from the same topic. This shows the basic idea of asynchronous communication in event-driven architecture.

## How to Run

- Start Kafka with Docker Compose.
- Run the consumer.
- Run the producer.

## Note on Running Producer vs Consumer

This project uses a single `mainClass` setting in `pom.xml` to decide which 
application runs with `mvn exec:java`. By default, it's set to run the Producer.

To switch between them, open `pom.xml` and change the `<mainClass>` value inside 
the `exec-maven-plugin` configuration:

- For the Consumer: `com.i2iacademy.kafka.OrderConsumer`
- For the Producer: `com.i2iacademy.kafka.OrderProducer`

To see the full flow, run the Consumer first (in one terminal), then run the 
Producer (in a second terminal) — the Consumer will print the messages as they 
arrive.
