package rt.kafka.deser_message.run;

import rt.kafka.deser_message.KafkaMessageDeserService;
import rt.kafka.deser_message.xml.XmlMessageDeser;

import java.util.Properties;

public class RunXmlConsumer {
    public static void main(String[] args) {
        String topicName = "xml02";

        Properties config = new Properties();
        config.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
        config.setProperty("group.id", "xml012");
        config.setProperty("enable.auto.commit", "true");
        config.setProperty("auto.offset.reset", "earliest");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaMessageDeserService service = new XmlMessageDeser(topicName, config);

        service.runConsumer();
    }
}
