package rt.kafka.Service.jsonImpl;

import org.apache.kafka.clients.producer.ProducerRecord;
import rt.kafka.Service.BuildSerilizationSchema;

import javax.annotation.Nullable;

public class JsonSerialization extends BuildSerilizationSchema<String> {
    private final String topicName;

    public JsonSerialization(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String message, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]> (
                topicName, message.getBytes()
        );
    }
}
