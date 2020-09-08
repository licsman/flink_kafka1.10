package rt.kafka.produce_service.csvImpl;

import org.apache.kafka.clients.producer.ProducerRecord;
import rt.kafka.produce_service.BuildSerilizationSchema;

import javax.annotation.Nullable;

public class CsvSerialization extends BuildSerilizationSchema<byte[]> {
    private final String topicName;

    public CsvSerialization(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(byte[] message, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]> (
                topicName, message
        );
    }
}
