package rt.kafka.deser_message.run;

import rt.kafka.deser_message.avro_registry.AvroRegistryMessageDeser;

import java.util.Properties;

public class RunAvroRegistryConsumer {
    public static void main(String[] args) {
        String topicName = "avro.registry.01";
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
        kafkaConfig.setProperty("group.id", "registry02");
        kafkaConfig.setProperty("enable.auto.commit", "true");
        kafkaConfig.setProperty("auto.offset.reset", "earliest");
        kafkaConfig.setProperty("auto.commit.interval.ms", "1000");
        kafkaConfig.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConfig.setProperty("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
        kafkaConfig.setProperty("schema.registry.url", "http://172.20.3.63:8091");

        AvroRegistryMessageDeser avroRegistryMessageDeser = new AvroRegistryMessageDeser(topicName, kafkaConfig);

        avroRegistryMessageDeser.runConsumer();
    }
}
