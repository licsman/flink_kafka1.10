package rt.kafka.produce_service.avro_schema_registry;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class RunAvroRegistryEmit {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int total = 1;
        String topicName = "avro.registry.01";
        Properties kafkaConfig = new Properties();

        kafkaConfig.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
        kafkaConfig.setProperty("key.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaConfig.setProperty("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaConfig.setProperty("schema.registry.url", "http://172.20.3.63:8091");

        AvroRegistryMessageEmit avroRegistryMessageEmit = new AvroRegistryMessageEmit();
        avroRegistryMessageEmit.send(total, topicName, kafkaConfig);
    }
}
