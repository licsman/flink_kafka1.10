package rt.kafka.produce_service.model;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Product {
    private int productId;
    private String productName;
    private double productPrice;
    private int productWeight;
    private String productDescription;

    public Product(int productId, String productName, double productPrice, int productWeight, String productDescription) {
        this.productId = productId;
        this.productName = productName;
        this.productPrice = productPrice;
        this.productWeight = productWeight;
        this.productDescription = productDescription;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public void setProductPrice(double productPrice) {
        this.productPrice = productPrice;
    }

    public void setProductWeight(int productWeight) {
        this.productWeight = productWeight;
    }

    public void setProductDescription(String productDescription) {
        this.productDescription = productDescription;
    }

    @Override
    public String toString() {
        return "Product{" +
                "productId=" + productId +
                ", productName='" + productName + '\'' +
                ", productPrice=" + productPrice +
                ", productWeight=" + productWeight +
                ", productDescription='" + productDescription + '\'' +
                '}';
    }

    public byte[] toCsvByte() {
        return String.format("%s,%s,%s,%s,%s",
                productId,
                productName,
                productPrice,
                productWeight,
                '"'+productDescription+'"').getBytes();
    }

    public byte[] toJsonByte() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("productId", productId);
        jsonObject.put("productName", productName);
        jsonObject.put("productPrice", productPrice);
        jsonObject.put("productWeight", productWeight);
        jsonObject.put("productDescription", productDescription);
        return jsonObject.toJSONString().getBytes();
    }

    public byte[] toAvroByte() {
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
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 2 * 1024);
            Schema schema = new Schema.Parser().parse(schemaStr);
            GenericRecord record = new GenericData.Record(schema);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            EncoderFactory encoderFactory = EncoderFactory.get();
            BinaryEncoder encoder = encoderFactory.binaryEncoder(out, null);
            writeDataToRecord(schemaStr, record);
            writer.write(record, encoder);
            encoder.flush();
            out.flush();
            return out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void writeDataToRecord(String schemaStr, GenericRecord record) {
        JSONObject jsonObject = JSONObject.parseObject(schemaStr);
        record.put("productId", productId);
        record.put("productName", productName);
        record.put("productPrice", productPrice);
        record.put("productWeight", productWeight);
        record.put("productDescription", productDescription);
    }
}
