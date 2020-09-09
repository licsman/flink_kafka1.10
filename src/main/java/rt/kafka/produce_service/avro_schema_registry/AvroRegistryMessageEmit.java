package rt.kafka.produce_service.avro_schema_registry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import rt.kafka.produce_service.model.PRODUCT_TEMPLATE;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AvroRegistryMessageEmit {

    public void send(int total, String topicName, Properties kafkaConfig) throws InterruptedException, ExecutionException {
        KafkaProducer<String, GenericRecord> kafkaProducer = new KafkaProducer<String, GenericRecord>(kafkaConfig);
        String schemaStr = "{\n" +
                "\"type\": \"record\",\n" +
                "\"name\": \"Product\",\n" +
                "\"namespace\": \"productAvro\",\n" +
                "\"fields\": [\n" +
                "{ \"name\": \"productId\", \"type\": \"int\"},\n" +
                "{ \"name\": \"productName\", \"type\": \"string\"},\n" +
                "{ \"name\": \"productPrice\", \"type\": \"double\"},\n" +
                "{ \"name\": \"productWeight\", \"type\": \"int\"},\n" +
                "{ \"name\": \"productDescription\", \"type\": \"string\"}\n" +
                "]\n" +
                "}";
        Schema schema = new Schema.Parser().parse(schemaStr);
        GenericRecord record = new GenericData.Record(schema);

        while (total < 500) {
            ProducerRecord<String,GenericRecord> message = buildRecord(topicName, record, total);
            kafkaProducer.send(message).get();
            Thread.sleep(500);
            total ++;
        }

    }

    private ProducerRecord<String,GenericRecord> buildRecord(String topicName, GenericRecord record, int productId) {
        PRODUCT_TEMPLATE template = PRODUCT_TEMPLATE.valueOf("P" + (productId % 10));
        record.put("productId", template.getId());
        record.put("productName", template.getName());
        record.put("productPrice", template.getPrice());
        record.put("productWeight", template.getWeight());
        record.put("productDescription", template.getDescription());

        System.out.println("发送了第" + productId + "条数据！");
        return new ProducerRecord<String, GenericRecord>(topicName, record);
    }
}
