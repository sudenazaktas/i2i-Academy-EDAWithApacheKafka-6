package com.i2iacademy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {

        // 1. Configure how we connect to Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Create the Producer instance
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 3. Simulate a "custom object" by converting an Order into a JSON-like string
        String topic = "orders-topic";

        for (int i = 1; i <= 5; i++) {
            String orderId = "ORD-" + i;
            String orderJson = String.format(
                "{\"orderId\":\"%s\",\"product\":\"Laptop\",\"quantity\":%d}",
                orderId, i
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, orderId, orderJson);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Sent -> Partition: " + metadata.partition()
                            + ", Offset: " + metadata.offset() + ", Message: " + orderJson);
                } else {
                    exception.printStackTrace();
                }
            });
        }

        // 4. Wait until all queued messages are actually sent, then close the connection
        producer.flush();
        producer.close();

        System.out.println("All messages sent, Producer closed.");
    }
}