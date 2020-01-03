package com.company;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class KafkaDataProducer {
    public static void main(String[] args) {

        System.out.println("this is kafka producer-");

        String topic = "topics";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        System.out.println("Creating kafka prod");


        KafkaProducer<Long, String> kafkaproducer = new KafkaProducer<>(props);


        System.out.println("sending message started");
        for (int index = 0; index < 100; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic, "This is record " + index);
            try {
                RecordMetadata metadata = kafkaproducer.send(record).get();
                System.out.println(
                        "Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            }
            catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
            catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }


    }
}
