package rt.kafka.deser_message.json;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import rt.kafka.deser_message.KafkaMessageDeserService;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class JsonMessageDeser implements KafkaMessageDeserService {

    private final String topicName;
    private final Properties kafkaConfig;

    public JsonMessageDeser(String topicName, Properties properties) {
        this.topicName = topicName;
        this.kafkaConfig = properties;
    }

    @Override
    public void runConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConfig);
        consumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("partition = %s, offset = %d, key = %s, value = %s%n",record.partition() ,record.offset(), record.key(), record.value());
        }
    }
}
