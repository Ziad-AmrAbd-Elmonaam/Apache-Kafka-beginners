package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBacks {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoCallBacks.class);

    public static void main(String[] args) {
        log.info("hello I am producer");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "coherent-lynx-14647-eu2-kafka.upstash.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y29oZXJlbnQtbHlueC0xNDY0NyTyujMqwFw2USF7JQ1pt6y7NXJA4BwoZFseJwA\" password=\"NGE0MWE1MjYtZDJmMS00OGQwLWFiZjYtNzJhNWU4NTQxN2M1\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("batch.size", "400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world "+ i);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception== null)
                        {
                            log.info("received new meta data \n" +
                                    "Topic: "+metadata.topic()+"\n"+
                                    "Topic: "+metadata.topic()+"\n"+
                                    "Partition: "+metadata.partition()+"\n"+
                                    "offset: "+metadata.offset()+"\n"+
                                    "timestamp: "+metadata.timestamp());

                        }
                        else {
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        producer.flush();
        producer.close();
    }
}
