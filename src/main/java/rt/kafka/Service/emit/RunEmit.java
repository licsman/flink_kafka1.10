package rt.kafka.Service.emit;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import rt.kafka.Service.KafkaMessageEmitService;
import rt.kafka.Service.csvImpl.CsvMessageEmit;
import rt.kafka.Service.jsonImpl.JsonMessageEmit;

import java.util.Properties;

public class RunEmit {
    public static void main(String[] args) throws Exception {
        String topicName = "json02";
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
//        KafkaMessageEmitService<String> kafkaMessageEmitService = new CsvMessageEmit(topicName, kafkaConfig);
        KafkaMessageEmitService<String> kafkaMessageEmitService = new JsonMessageEmit(topicName, kafkaConfig);

        kafkaMessageEmitService.initSerializationSchema(topicName);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.addSource(new MessagesProduct("json"));

        stream.addSink(kafkaMessageEmitService.runProducer());

        env.execute("kafkaMoni");
    }
}
