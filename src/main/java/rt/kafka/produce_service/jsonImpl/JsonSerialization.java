package rt.kafka.produce_service.jsonImpl;

import org.apache.kafka.clients.producer.ProducerRecord;
import rt.kafka.produce_service.BuildSerilizationSchema;

import javax.annotation.Nullable;

public class JsonSerialization extends BuildSerilizationSchema<byte[]> {
    private final String topicName;

    public JsonSerialization(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(byte[] message, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]> (
                topicName, message
        );
    }
}
