package rt.kafka.deser_message.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import rt.kafka.deser_message.KafkaMessageDeserService;
import rt.kafka.produce_service.model.Product;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroMessageDeser implements KafkaMessageDeserService {
    private final String topicName;

    private final Properties kafkaConfig;

    public AvroMessageDeser(String topicName, Properties properties) {
        this.topicName = topicName;
        this.kafkaConfig = properties;
    }

    @Override
    public void runConsumer() {
        String schemaStr = "{\n" +
                "\"type\": \"record\",\n" +
                "\"name\": \"Employee\",\n" +
                "\"fields\": [\n" +
                "{ \"name\": \"productId\", \"type\": \"int\"},\n" +
                "{ \"name\": \"productName\", \"type\": \"string\"},\n" +
                "{ \"name\": \"productPrice\", \"type\": \"double\"},\n" +
                "{ \"name\": \"productWeight\", \"type\": \"int\"},\n" +
                "{ \"name\": \"productDescription\", \"type\": \"string\"}\n" +
                "]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaStr);
        GenericRecord recorder = new GenericData.Record(schema);
        SpecificDatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaConfig);
        consumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
                try {
                    while (!decoder.isEnd()) {
                        reader.read(recorder, decoder);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.printf("partition = %s, offset = %d, value = %s%n",
                        record.partition(),
                        record.offset(),
                        new Product(
                                (int)recorder.get("productId"),
                                String.valueOf(recorder.get("productName")),
                                (double)recorder.get("productPrice"),
                                (int)recorder.get("productWeight"),
                                String.valueOf(recorder.get("productDescription"))
                        ));
            }
        }
    }
}
