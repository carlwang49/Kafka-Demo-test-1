package io.conduktor.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {

        String bootstrapServers = "localhost:9092";
        String groupId = "my-group";
        String topic = "my-topic";

        log.info("consumer properties setting");

        //consumer properties setting

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //low latency setting

        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "0");
        properties.setProperty("enable.auto.commit", "false");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to our topics)
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(500));
            long currentTime = System.currentTimeMillis();

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Value: " + record.value() +
                        "   Timestamp: " + record.timestamp() +
                        "   Latency: " + (currentTime - record.timestamp()) + " ms");
            }
        }
    }
}
