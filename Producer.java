package io.conduktor.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {

        log.info("Properties setting");
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // make low latency properties
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(2*1024));
        properties.put(ProducerConfig.ACKS_CONFIG, "0");

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        log.info("Start producer");
        for (int i = 0; i < 1000; i++) {

            //create a producer record
            ProducerRecord<String, String> producerRecord
                    = new ProducerRecord<>("my-topic", 0, System.currentTimeMillis(), null, "Hello NCKU - " + i);

            //set the data - asynchronous
            System.out.println("record_timestamp: " + producerRecord.timestamp());
            producer.send(producerRecord);

            //flush data - synchronous
            producer.flush();

            //sleep 1 ms
            TimeUnit.MILLISECONDS.sleep(1000);
        }
    }
}
