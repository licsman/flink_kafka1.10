package rt.kafka.produce_service.jsonImpl;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import rt.kafka.produce_service.KafkaMessageEmitService;

import java.util.Properties;

public class JsonMessageEmit implements KafkaMessageEmitService<byte[]> {

    private final String topicName;

    private final Properties kafkaConfigs;

    private JsonSerialization jsonSerialization;

    public JsonMessageEmit(String topicName, Properties kafkaConfigs) {
        this.topicName = topicName;
        this.kafkaConfigs = kafkaConfigs;
    }

    @Override
    public void initSerializationSchema(String topicName) {
        jsonSerialization = new JsonSerialization(topicName);
    }

    @Override
    public FlinkKafkaProducer<byte[]> runProducer() {
        return new FlinkKafkaProducer<byte[]>(
                topicName,
                jsonSerialization,
                kafkaConfigs,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
