package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        log.info("hello I am Consumer!!");
        String groupId = "my-java-application";
        String topic = "java_demo";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "coherent-lynx-14647-eu2-kafka.upstash.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Y29oZXJlbnQtbHlueC0xNDY0NyTyujMqwFw2USF7JQ1pt6y7NXJA4BwoZFseJwA\" password=\"NGE0MWE1MjYtZDJmMS00OGQwLWFiZjYtNzJhNWU4NTQxN2M1\";");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");


        KafkaConsumer <String,String> consumer =new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        while (true)
        {
            log.info("polling");
      ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String,String>record :records)    {
          log.info("key: "+record.key()+"value:"+record.value());
          log.info("partition: "+record.partition()+"offsets:"+record.offset());

      }
        }




    }
}
