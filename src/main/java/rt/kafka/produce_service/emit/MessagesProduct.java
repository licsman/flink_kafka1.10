package rt.kafka.produce_service.emit;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import rt.kafka.produce_service.model.PRODUCT_TEMPLATE;
import rt.kafka.produce_service.model.Product;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.ByteArrayOutputStream;

import static java.lang.Thread.sleep;

public class MessagesProduct implements SourceFunction<byte[]> {
    private volatile boolean isRunning = true;
    private String flag = "csv";
    private int count = 0;

    public MessagesProduct(String flag) {
        this.flag = flag;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) throws Exception {
        switch (flag) {
            case "csv" :
                CsvMessage(ctx, 100);
            case "json" :
                JsonMessage(ctx, 100);
            case "avro" :
                AvroMessage(ctx, 100);
            case "xml" :
                XmlMessage(ctx, 100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public void CsvMessage(SourceContext<byte[]> ctx, int total) throws InterruptedException {
        while (isRunning && count < total) {
            PRODUCT_TEMPLATE template = PRODUCT_TEMPLATE.valueOf("P" + (count%10));
            Product product = new Product(
                    template.getId(),
                    template.getName(),
                    template.getPrice(),
                    template.getWeight(),
                    template.getDescription()
            );
            ctx.collect(product.toCsvByte());
            System.out.println("csv生产第" + (count + 1) + "条：" + product.toString());
            sleep(1000);
            count ++;
        }
    }

    public void JsonMessage(SourceContext<byte[]> ctx, int total) throws InterruptedException {
        while (isRunning && count < total) {
            PRODUCT_TEMPLATE template = PRODUCT_TEMPLATE.valueOf("P" + (count%10));
            Product product = new Product(
                    template.getId(),
                    template.getName(),
                    template.getPrice(),
                    template.getWeight(),
                    template.getDescription()
            );
            ctx.collect(product.toJsonByte());
            System.out.println("json生产第" + (count + 1) + "条：" + product.toString());
            sleep(1000);
            count ++;
        }
    }

    public void AvroMessage(SourceContext<byte[]> ctx, int total) throws InterruptedException {
        while (isRunning && count < total) {
            PRODUCT_TEMPLATE template = PRODUCT_TEMPLATE.valueOf("P" + (count%10));
            Product product = new Product(
                    template.getId(),
                    template.getName(),
                    template.getPrice(),
                    template.getWeight(),
                    template.getDescription()
            );
            ctx.collect(product.toAvroByte());
            System.out.println("avro生产第" + (count + 1) + "条：" + product.toString());
            sleep(1000);
            count ++;
        }
    }

    public void XmlMessage(SourceContext<byte[]> ctx, int total) throws InterruptedException {
        while (isRunning && count < total) {
            PRODUCT_TEMPLATE template = PRODUCT_TEMPLATE.valueOf("P" + (count%10));
            Product product = new Product(
                    template.getId(),
                    template.getName(),
                    template.getPrice(),
                    template.getWeight(),
                    template.getDescription()
            );
            ctx.collect(toXmlByte(product));
            System.out.println("xml生产第" + (count + 1) + "条：" + product.toString());
            sleep(1000);
            count ++;
        }
    }

    public byte[] toXmlByte(Product product) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            JAXBContext context = JAXBContext.newInstance(Product.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            marshaller.marshal(product, baos);
            System.out.println("输出xml:" + new String(baos.toByteArray()));
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return baos.toByteArray();
    }
}
