package rt.kafka.produce_service;


import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BuildSerilizationSchema<IN> implements KafkaSerializationSchema<IN> {

    @Override
    public ProducerRecord<byte[], byte[]> serialize(IN element, @Nullable Long timestamp) {
        return null;
    }
}
