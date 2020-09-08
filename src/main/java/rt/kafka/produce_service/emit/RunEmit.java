package rt.kafka.produce_service.emit;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import rt.kafka.produce_service.KafkaMessageEmitService;
import rt.kafka.produce_service.avroImpl.AvroMessageEmit;

import java.util.Properties;

public class RunEmit {
    public static void main(String[] args) throws Exception {
        String topicName = "avro02";
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
//        KafkaMessageEmitService<byte[]> kafkaMessageEmitService = new CsvMessageEmit(topicName, kafkaConfig);
//        KafkaMessageEmitService<byte[]> kafkaMessageEmitService = new JsonMessageEmit(topicName, kafkaConfig);
        KafkaMessageEmitService<byte[]> kafkaMessageEmitService = new AvroMessageEmit(topicName, kafkaConfig);

        kafkaMessageEmitService.initSerializationSchema(topicName);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<String> stream = env.addSource(new MessagesProduct("json"));
        DataStreamSource<byte[]> stream = env.addSource(new MessagesProduct("avro"));

        stream.addSink(kafkaMessageEmitService.runProducer());

        env.execute("kafkaMoni");
    }
}
