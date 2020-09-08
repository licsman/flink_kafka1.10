package rt.kafka.produce_service.csvImpl;


import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import rt.kafka.produce_service.KafkaMessageEmitService;

import java.util.Properties;

public class CsvMessageEmit implements KafkaMessageEmitService<byte[]> {

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
    public FlinkKafkaProducer<byte[]> runProducer() {

        return new FlinkKafkaProducer<byte[]>(
                topicName,
                csvSerialization,
                kafkaConfigs,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
