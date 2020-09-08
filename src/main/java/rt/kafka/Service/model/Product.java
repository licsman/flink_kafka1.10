package rt.kafka.Service.model;

import com.alibaba.fastjson.JSONObject;

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

    public String toCsvStr() {
        return productId + "," + productName + "," + productPrice + "," + productWeight + "," + '"' + productDescription + '"';
    }

    public String toJsonStr() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("productId", productId);
        jsonObject.put("productName", productName);
        jsonObject.put("productPrice", productPrice);
        jsonObject.put("productWeight", productWeight);
        jsonObject.put("productDescription", productDescription);
        return jsonObject.toJSONString();
    }
}
