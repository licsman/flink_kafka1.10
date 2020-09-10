package rt.kafka.deser_message.xml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import rt.kafka.deser_message.KafkaMessageDeserService;
import rt.kafka.produce_service.model.Product;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class XmlMessageDeser implements KafkaMessageDeserService {
    private final String topicName;

    private final Properties kafkaConfig;

    public XmlMessageDeser(String topicName, Properties kafkaConfig) {
        this.topicName = topicName;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void runConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConfig);
        consumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                XmlMapper xmlMapper = new XmlMapper();
                xmlMapper.setDefaultUseWrapper(false);
                //自动忽略无法对应pojo的字段
                xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                try {
                    Product product = xmlMapper.readValue(record.value(), Product.class);
                    System.out.printf("partition = %s, offset = %d, key = %s, value = %s%n",
                            record.partition() ,record.offset(), record.key(), product);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
