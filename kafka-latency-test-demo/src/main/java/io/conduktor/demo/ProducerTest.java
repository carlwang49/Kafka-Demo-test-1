package io.conduktor.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class ProducerTest {

    private static final Logger log = LoggerFactory.getLogger(ProducerTest.class.getSimpleName());

    String topicName = "second-topic";
    KafkaProducer<String, String> kafkaProducer;

    public ProducerTest(Map<String, Object> propsMap) {

        kafkaProducer = new KafkaProducer<String, String>(propsMap);
    }

    public static Map<String, Object> propsMap() {

        log.info("consumer properties setting");

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        propsMap.put(ProducerConfig.ACKS_CONFIG, "0");
        propsMap.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 0);
        return propsMap;
    }

    public void publishMessageSync(String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);

            long producerCurrentTimestamp = System.currentTimeMillis();
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            if (recordMetadata.timestamp() - producerCurrentTimestamp == 0){
                System.out.println("partition_" + recordMetadata.partition() +
                        " , record_timestamp " + recordMetadata.timestamp());
            }else {
                System.out.println("Wrong producer timestamp!");
                System.out.println("please enter your message again! ");
                System.out.println("============================");
            }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Scanner scanner = new Scanner(System.in);
        ProducerTest messageProducer = new ProducerTest(propsMap());
        while (true) {
            System.out.print("enter your message: ");
            String message = scanner.next();
            messageProducer.publishMessageSync(null, message);
        }
    }
}
