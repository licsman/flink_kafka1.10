package rt.kafka.produce_service.avroImpl;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import rt.kafka.produce_service.KafkaMessageEmitService;

import java.util.Properties;

public class AvroMessageEmit implements KafkaMessageEmitService<byte[]> {

    private final String topicName;

    private final Properties kafkaConfigs;

    private AvroSerialization avroSerialization;

    public AvroMessageEmit(String topicName, Properties kafkaConfigs) {
        this.topicName = topicName;
        this.kafkaConfigs = kafkaConfigs;
    }

    @Override
    public void initSerializationSchema(String topicName) {
        avroSerialization = new AvroSerialization(topicName);
    }

    @Override
    public FlinkKafkaProducer<byte[]> runProducer() {
        return new FlinkKafkaProducer<byte[]>(
                topicName,
                avroSerialization,
                kafkaConfigs,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
