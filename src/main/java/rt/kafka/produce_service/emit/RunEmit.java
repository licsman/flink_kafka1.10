package rt.kafka.produce_service.emit;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import rt.kafka.produce_service.KafkaMessageEmitService;
import rt.kafka.produce_service.avroImpl.AvroMessageEmit;
import rt.kafka.produce_service.xmlImpl.XmlMessageEmit;

import java.util.Properties;

public class RunEmit {
    public static void main(String[] args) throws Exception {
        String topicName = "xml02";
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
        //csv
        //KafkaMessageEmitService<byte[]> kafkaMessageEmitService = new CsvMessageEmit(topicName, kafkaConfig);

        //json
        //  KafkaMessageEmitService<byte[]> kafkaMessageEmitService = new JsonMessageEmit(topicName, kafkaConfig);

        //avro
        //KafkaMessageEmitService<byte[]> kafkaMessageEmitService = new AvroMessageEmit(topicName, kafkaConfig);

        //xml
        KafkaMessageEmitService<byte[]> kafkaMessageEmitService = new XmlMessageEmit(topicName, kafkaConfig);

        //初始 序列化schema
        kafkaMessageEmitService.initSerializationSchema(topicName);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<byte[]> stream = env.addSource(new MessagesProduct("xml"));

        stream.addSink(kafkaMessageEmitService.runProducer());

        env.execute("kafkaProduce");
    }
}
