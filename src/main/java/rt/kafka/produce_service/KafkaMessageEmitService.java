package rt.kafka.produce_service;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public interface KafkaMessageEmitService<IN> {

    void initSerializationSchema(String topicName);

    FlinkKafkaProducer<IN> runProducer();

}
