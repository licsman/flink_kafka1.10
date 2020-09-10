package rt.kafka.produce_service.xmlImpl;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import rt.kafka.produce_service.KafkaMessageEmitService;

import java.util.Properties;

public class XmlMessageEmit implements KafkaMessageEmitService<byte[]> {
    private final String topicName;

    private final Properties kafkaConfigs;

    private XmlSerialization xmlSerialization;

    public XmlMessageEmit(String topicName, Properties kafkaConfigs) {
        this.topicName = topicName;
        this.kafkaConfigs = kafkaConfigs;
    }

    @Override
    public void initSerializationSchema(String topicName) {
        xmlSerialization = new XmlSerialization(topicName);
    }

    @Override
    public FlinkKafkaProducer<byte[]> runProducer() {
        return new FlinkKafkaProducer<byte[]>(
                topicName,
                xmlSerialization,
                kafkaConfigs,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
