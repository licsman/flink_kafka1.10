package rt.kafka.Service.csvImpl;

import org.apache.kafka.clients.producer.ProducerRecord;
import rt.kafka.Service.BuildSerilizationSchema;

import javax.annotation.Nullable;

public class CsvSerialization extends BuildSerilizationSchema<String> {
    private final String topicName;

    public CsvSerialization(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String message, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]> (
                topicName, message.getBytes()
        );
    }
}
