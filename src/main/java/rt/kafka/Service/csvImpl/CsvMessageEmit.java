package rt.kafka.Service.csvImpl;


import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import rt.kafka.Service.KafkaMessageEmitService;

import java.util.Properties;

public class CsvMessageEmit implements KafkaMessageEmitService<String> {

    private final String topicName;

    private CsvSerialization csvSerialization;

    private final Properties kafkaConfigs;

    public CsvMessageEmit(String topic, Properties kafkaConfigs) {
        this.topicName = topic;
        this.kafkaConfigs = kafkaConfigs;
    }

    @Override
    public void initSerializationSchema(String topicName) {
        csvSerialization = new CsvSerialization(topicName);
    }

    @Override
    public FlinkKafkaProducer<String> runProducer() {

        return new FlinkKafkaProducer<String>(
                topicName,
                csvSerialization,
                kafkaConfigs,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
