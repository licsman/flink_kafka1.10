package rt.kafka.deser_message.run;

import rt.kafka.deser_message.KafkaMessageDeserService;
import rt.kafka.deser_message.avro.AvroMessageDeser;
import rt.kafka.deser_message.json.JsonMessageDeser;

import java.util.Properties;

public class RunAvroConsumer {
    public static void main(String[] args) {
        String topicName = "avro02";

        Properties config = new Properties();
        config.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
        config.setProperty("group.id", "avrotest01");
        config.setProperty("enable.auto.commit", "true");
        config.setProperty("auto.offset.reset", "earliest");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        config.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaMessageDeserService service = new AvroMessageDeser(topicName, config);

        service.runConsumer();
    }
}
