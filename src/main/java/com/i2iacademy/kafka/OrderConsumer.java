package com.i2iacademy.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {

        // 1. Configure how we connect to Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 2. Define which consumer group this consumer belongs to
        //    (consumers in the same group share the work of reading messages)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-consumer-group");

        // 3. If we've never read from this topic before, start from the very beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 4. Create the Consumer instance and subscribe to the topic
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("orders-topic"));

        System.out.println("Consumer started, waiting for messages... (press Ctrl+C to stop)");

        // 5. Continuously poll for new messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received -> Partition: " + record.partition()
                        + ", Offset: " + record.offset()
                        + ", Key: " + record.key()
                        + ", Value: " + record.value());
            }
        }
    }
}