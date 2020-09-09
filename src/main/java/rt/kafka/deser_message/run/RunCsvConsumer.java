package rt.kafka.deser_message.run;

import rt.kafka.deser_message.KafkaMessageDeserService;
import rt.kafka.deser_message.csv.CsvMessageDeser;

import java.util.Properties;

public class RunCsvConsumer {
    public static void main(String[] args) {
        String topicName = "csv02";

        Properties config = new Properties();
        config.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
        config.setProperty("group.id", "csvtest02");
        config.setProperty("enable.auto.commit", "false");
        config.setProperty("auto.offset.reset", "earliest");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaMessageDeserService service = new CsvMessageDeser(topicName, config);

        service.runConsumer();
    }
}
