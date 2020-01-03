package com.company;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import scala.collection.immutable.Stream;

import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        System.out.println("this is kafka streams");

        String topic = "topics";
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsbuilder = new StreamsBuilder();
        KStream<Integer, String> kstream = streamsbuilder.stream(topic);
        kstream.foreach((k, v) -> System.out.println("message: "+k+":"+v));

        // Build topology
        Topology topology = streamsbuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println("Stream registered ");

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            streams.close();
            System.out.println("Shutting down the stream");
        }));

    }
}
