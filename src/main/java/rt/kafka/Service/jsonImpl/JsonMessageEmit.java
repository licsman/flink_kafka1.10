package rt.kafka.Service.jsonImpl;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import rt.kafka.Service.KafkaMessageEmitService;

import java.util.Properties;

public class JsonMessageEmit implements KafkaMessageEmitService<String> {

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
    public FlinkKafkaProducer<String> runProducer() {
        return new FlinkKafkaProducer<String>(
                topicName,
                jsonSerialization,
                kafkaConfigs,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
