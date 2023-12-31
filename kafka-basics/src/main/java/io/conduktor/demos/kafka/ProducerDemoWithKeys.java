package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {
        log.info("hello I am producer");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "coherent-lynx-14647-eu2-kafka.upstash.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y29oZXJlbnQtbHlueC0xNDY0NyTyujMqwFw2USF7JQ1pt6y7NXJA4BwoZFseJwA\" password=\"NGE0MWE1MjYtZDJmMS00OGQwLWFiZjYtNzJhNWU4NTQxN2M1\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0; j <2; j++) {
            for (int i = 0; i < 30; i++) {
                String topic = "java_demo";
                String key = "id_" +i;
                String value="hello osama"+i;


                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key,value);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception== null)
                        {
                            log.info(
                                    "Key: "+key +" | Partition: "+metadata.partition());

                        }
                        else {
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }




        producer.flush();
        producer.close();
    }
}
