package tests;

import com.sun.istack.internal.NotNull;
import operations.*;

public class TestHandler
{
    private static TestHandler testHandler = null;
    private ArticleOperations articleOperations;
    private BuyerOperations buyerOperations;
    private CityOperations cityOperations;
    private GeneralOperations generalOperations;
    private OrderOperations orderOperations;
    private ShopOperations shopOperations;
    private TransactionOperations transactionOperations;


    public TestHandler(@NotNull ArticleOperations articleOperations, @NotNull BuyerOperations buyerOperations, @NotNull CityOperations cityOperations, @NotNull GeneralOperations generalOperations, @NotNull OrderOperations orderOperations, @NotNull ShopOperations shopOperations, @NotNull TransactionOperations transactionOperations) {
        this.articleOperations = articleOperations;
        this.buyerOperations = buyerOperations;
        this.cityOperations = cityOperations;
        this.generalOperations = generalOperations;
        this.orderOperations = orderOperations;
        this.shopOperations = shopOperations;
        this.transactionOperations = transactionOperations;
    }

    public static void createInstance(@NotNull ArticleOperations articleOperations, @NotNull BuyerOperations buyerOperations, @NotNull CityOperations cityOperations, @NotNull GeneralOperations generalOperations, @NotNull OrderOperations orderOperations, @NotNull ShopOperations shopOperations, @NotNull TransactionOperations transactionOperations)
    {
        testHandler = new TestHandler(articleOperations, buyerOperations, cityOperations, generalOperations, orderOperations, shopOperations, transactionOperations);
    }

    static TestHandler getInstance()
    {
        return testHandler;
    }

    public ArticleOperations getArticleOperations() {
        return articleOperations;
    }

    public BuyerOperations getBuyerOperations() {
        return buyerOperations;
    }

    public CityOperations getCityOperations() {
        return cityOperations;
    }

    public GeneralOperations getGeneralOperations() {
        return generalOperations;
    }

    public OrderOperations getOrderOperations() {
        return orderOperations;
    }

    public ShopOperations getShopOperations() {
        return shopOperations;
    }

    public TransactionOperations getTransactionOperations() {
        return transactionOperations;
    }
}
