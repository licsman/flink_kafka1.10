package rt.kafka.deser_message.avro_registry;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import rt.kafka.deser_message.KafkaMessageDeserService;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroRegistryMessageDeser implements KafkaMessageDeserService {
    private final String topicName;
    private final Properties kafkaConfig;

    public AvroRegistryMessageDeser(String topicName, Properties kafkaConfig) {
        this.topicName = topicName;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void runConsumer() {
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(kafkaConfig);
        consumer.subscribe(Collections.singletonList(topicName));
        while (true)
        {
            ConsumerRecords<String,  GenericRecord> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, GenericRecord> record : records) {
                try {
                    System.out.println(record.value().get("productName"));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
