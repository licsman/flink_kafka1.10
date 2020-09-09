package rt.kafka.deser_message.csv;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import rt.kafka.deser_message.KafkaMessageDeserService;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CsvMessageDeser implements KafkaMessageDeserService {

    private final Properties kafkaconfig;

    private final String topicName;

    public CsvMessageDeser(String topicName, Properties properties) {
        this.kafkaconfig = properties;
        this.topicName = topicName;
    }

    @Override
    public void runConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaconfig);
        consumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("partition = %s, offset = %d, key = %s, value = %s%n",record.partition() ,record.offset(), record.key(), record.value());
        }
    }
}
