package io.conduktor.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaLatencyTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaLatencyTest.class.getSimpleName());
    private static final String topicName = "second-topic";
    private static final String bootstrapServers = "localhost:9092";
    private static final String groupId = "my-group";
    private long producerSendTimestamp;
    public static String message;
    public static Thread thread;
    static KafkaProducer<String, String> kafkaProducer;
    static KafkaConsumer<String, String> kafkaConsumer;

    public KafkaLatencyTest(Map<String, Object> propsMap1, Map<String, Object> propsMap2) {

        //initialize Kafka properties
        kafkaConsumer = new KafkaConsumer<>(propsMap1);
        kafkaProducer = new KafkaProducer<>(propsMap2);
    }

    public static Map<String, Object> consumerPropsMap() {

        log.info("consumer properties setting");

        Map<String, Object> propsMap = new HashMap<>();

        // consumer properties setting
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //low latency setting

        propsMap.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        propsMap.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "0");
        propsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        return propsMap;
    }

    public static Map<String, Object> producerPropsMap() {

        log.info("producer properties setting");

        // producer properties setting

        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //low latency setting

        propsMap.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        propsMap.put(ProducerConfig.ACKS_CONFIG, "0");
        propsMap.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        propsMap.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 0);

        return propsMap;
    }

    public void startConsumeMessage() {
        thread = new Thread(() -> {
            while (true){
                // poll for new data....
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(100));
                // record the timestamp when the message consume by consumer
                long consumeTimestamp = System.currentTimeMillis();

                System.out.println("\n==================== consume data return ====================");
                System.out.println("current_record_count: " + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("record_value: " + record.value() +
                            "   record_partition_" + record.partition() +
                            "   Latency: " + (consumeTimestamp - producerSendTimestamp) + " ms");
                }
                System.out.println("===============================================================");
                System.out.print("enter your message: ");
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    public void publishMessageAsync(String key, String value) {

        // create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        // kafkaProducer send -- asynchronous
        kafkaProducer.send(producerRecord);
        // start sending the message to kafka broker
        kafkaProducer.flush();
        // record the timestamp when producer start sending...
        producerSendTimestamp = System.currentTimeMillis();
    }

    public static void main(String[] args) throws InterruptedException {

        // KafkaLatencyTest Initialize...
        KafkaLatencyTest messageProducer = new KafkaLatencyTest(consumerPropsMap(), producerPropsMap());

        // subscribe consumer to our topic
        kafkaConsumer.subscribe(Collections.singleton(topicName));

        // start polling data from broker
        messageProducer.startConsumeMessage();

        // wait for consumer initialization completed
        TimeUnit.SECONDS.sleep(10);

        // prompt the user to enter their message
        Scanner scanner = new Scanner(System.in);
        System.out.print("enter your message: ");

        do {
            message = scanner.next();
            if (message.equals("exit")){
                break;
            }else {
                // call the method --publishMessageAsync
                messageProducer.publishMessageAsync(null, message);
            }
        } while (!message.equals("exit"));

        // close the thread method --startConsumeMessage
        thread.stop();

        // close KafkaProducer
        kafkaProducer.close();

        // close KafkaConsumer
        kafkaConsumer.close();
    }
}
