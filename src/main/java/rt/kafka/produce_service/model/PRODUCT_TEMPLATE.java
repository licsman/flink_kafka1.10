package rt.kafka.produce_service.model;

public enum PRODUCT_TEMPLATE {
    P0(0,"Apple12",6988.22,239, "good,good,good"),
    P1(1,"Apple11",5988.22,233, "good,good,good"),
    P2(2,"OPPO R15",3699.67,344, "good,good,good"),
    P3(3,"VIVO NEX11",3988.22,233, "good,good,good"),
    P4(4,"MEIZU 16XS",7988.22,233, "good,good,good"),
    P5(5,"HUAWEI P40",19988.22,233, "good,good,good"),
    P6(6,"XIAOMI 10",1988.22,233, "good,good,good"),
    P7(7,"OnePlus 7Pro",6988.22,233, "good,good,good"),
    P8(8,"HUAWEI P40",19988.22,233, "good,good,good"),
    P9(9,"XIAOMI 10",1988.22,233, "good,good,good"),
    P10(10,"OnePlus 7Pro",6988.22,233, "good,good,good");

    private final int id;
    private final String name;
    private final double price;
    private final int weight;
    private final String description;

    PRODUCT_TEMPLATE(int id, String name, double price, int weight, String description) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.weight = weight;
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getPrice() {
        return price;
    }

    public int getWeight() {
        return weight;
    }

    public String getDescription() {
        return description;
    }
}
