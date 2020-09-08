package rt.kafka.Service.emit;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import rt.kafka.Service.model.PRODUCT_TEMPLATE;
import rt.kafka.Service.model.Product;

import static java.lang.Thread.sleep;

public class MessagesProduct implements SourceFunction<String> {
    private volatile boolean isRunning = true;
    private String flag = "csv";
    private int count = 0;

    public MessagesProduct(String flag) {
        this.flag = flag;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        switch (flag) {
            case "csv" :
                CsvMessage(ctx, 100);
            case "json" :
                JsonMessage(ctx, 100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public void CsvMessage(SourceContext<String> ctx, int total) throws InterruptedException {
        while (isRunning && count < total) {
            PRODUCT_TEMPLATE template = PRODUCT_TEMPLATE.valueOf("P" + (count%10));
            Product product = new Product(
                    template.getId(),
                    template.getName(),
                    template.getPrice(),
                    template.getWeight(),
                    template.getDescription()
            );
            ctx.collect(product.toCsvStr());
            System.out.println("生产第" + (count + 1) + "条：" + product.toString());
            sleep(1000);
            count ++;
        }
    }

    public void JsonMessage(SourceContext<String> ctx, int total) throws InterruptedException {
        while (isRunning && count < total) {
            PRODUCT_TEMPLATE template = PRODUCT_TEMPLATE.valueOf("P" + (count%10));
            Product product = new Product(
                    template.getId(),
                    template.getName(),
                    template.getPrice(),
                    template.getWeight(),
                    template.getDescription()
            );
            ctx.collect(product.toJsonStr());
            System.out.println(product.toJsonStr());
            sleep(1000);
            count ++;
        }
    }
}
