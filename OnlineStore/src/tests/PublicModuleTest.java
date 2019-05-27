package tests;

import operations.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Calendar;

public class PublicModuleTest {

    private TestHandler testHandler;
    private GeneralOperations generalOperations;
    private ShopOperations shopOperations;
    private CityOperations cityOperations;
    private ArticleOperations articleOperations;
    private BuyerOperations buyerOperations;
    private OrderOperations orderOperations;
    private TransactionOperations transactionOperations;

    @Before
    public void setUp() throws Exception {
        this.testHandler = TestHandler.getInstance();
        Assert.assertNotNull(this.testHandler);

        this.shopOperations = this.testHandler.getShopOperations();
        Assert.assertNotNull(this.shopOperations);

        this.cityOperations = this.testHandler.getCityOperations();
        Assert.assertNotNull(this.cityOperations);

        this.articleOperations = this.testHandler.getArticleOperations();
        Assert.assertNotNull(this.articleOperations);

        this.buyerOperations = this.testHandler.getBuyerOperations();
        Assert.assertNotNull(this.buyerOperations);

        orderOperations = testHandler.getOrderOperations();
        Assert.assertNotNull(orderOperations);

        transactionOperations = testHandler.getTransactionOperations();
        Assert.assertNotNull(transactionOperations);

        generalOperations = testHandler.getGeneralOperations();
        Assert.assertNotNull(generalOperations);

        generalOperations.eraseAll();
    }

    @After
    public void tearDown() throws Exception {
        generalOperations.eraseAll();
    }

    @Test
    public void test(){
        Calendar initialTime = Calendar.getInstance();
        initialTime.clear();
        initialTime.set(2018, Calendar.JANUARY, 1);
        generalOperations.setInitialTime(initialTime);
        Calendar receivedTime = Calendar.getInstance();
        receivedTime.clear();
        receivedTime.set(2018, Calendar.JANUARY, 22);

        //make network
        int cityB = cityOperations.createCity("B");
        int cityC1 = cityOperations.createCity("C1");
        int cityA = cityOperations.createCity("A");
        int cityC2 = cityOperations.createCity("C2");
        int cityC3 = cityOperations.createCity("C3");
        int cityC4 = cityOperations.createCity("C4");
        int cityC5 = cityOperations.createCity("C5");

        cityOperations.connectCities(cityB, cityC1, 8);
        cityOperations.connectCities(cityC1, cityA, 10);
        cityOperations.connectCities(cityA, cityC2, 3);
        cityOperations.connectCities(cityC2, cityC3, 2);
        cityOperations.connectCities(cityC3, cityC4, 1);
        cityOperations.connectCities(cityC4, cityA, 3);
        cityOperations.connectCities(cityA, cityC5, 15);
        cityOperations.connectCities(cityC5, cityB, 2);

        //make shops, buyer and articles
        int shopA = shopOperations.createShop("shopA", "A");
        int shopC2 = shopOperations.createShop("shopC2", "C2");
        int shopC3 = shopOperations.createShop("shopC3", "C3");

        shopOperations.setDiscount(shopA, 20);
        shopOperations.setDiscount(shopC2, 50);

        int laptop = articleOperations.createArticle(shopA, "laptop", 1000);
        int monitor = articleOperations.createArticle(shopC2, "monitor", 200);
        int stolica = articleOperations.createArticle(shopC3, "stolica", 100);
        int sto = articleOperations.createArticle(shopC3, "sto", 200);

        shopOperations.increaseArticleCount(laptop, 10);
        shopOperations.increaseArticleCount(monitor, 10);
        shopOperations.increaseArticleCount(stolica, 10);
        shopOperations.increaseArticleCount(sto, 10);

        int buyer = buyerOperations.createBuyer("kupac", cityB);
        buyerOperations.increaseCredit(buyer, new BigDecimal("20000"));
        int order = buyerOperations.createOrder(buyer);

        orderOperations.addArticle(order, laptop, 5);
        orderOperations.addArticle(order, monitor, 4);
        orderOperations.addArticle(order, stolica, 10);
        orderOperations.addArticle(order, sto, 4);

        Assert.assertNull(orderOperations.getSentTime(order));
        Assert.assertTrue("created".equals(orderOperations.getState(order)));
        orderOperations.completeOrder(order);
        Assert.assertTrue("sent".equals(orderOperations.getState(order)));

        int buyerTransactionId = transactionOperations.getTransationsForBuyer(buyer).get(0);
        Assert.assertEquals(initialTime, transactionOperations.getTimeOfExecution(buyerTransactionId));

        Assert.assertNull(transactionOperations.getTransationsForShop(shopA));

        //calculate ammounts - begin
        BigDecimal shopAAmount = new BigDecimal("5").multiply(new BigDecimal("1000")).setScale(3);
        BigDecimal shopAAmountWithDiscount = new BigDecimal("0.8").multiply(shopAAmount).setScale(3);
        BigDecimal shopC2Amount = new BigDecimal("4").multiply(new BigDecimal("200")).setScale(3);
        BigDecimal shopC2AmountWithDiscount = new BigDecimal("0.5").multiply(shopC2Amount).setScale(3);
        BigDecimal shopC3Amount = (new BigDecimal("10").multiply(new BigDecimal("100")))
                .add(new BigDecimal("4").multiply(new BigDecimal("200"))).setScale(3);
        BigDecimal shopC3AmountWithDiscount = shopC3Amount;

        BigDecimal amountWithoutDiscounts = shopAAmount.add(shopC2Amount).add(shopC3Amount).setScale(3);
        BigDecimal amountWithDiscounts = shopAAmountWithDiscount.add(shopC2AmountWithDiscount).add(shopC3AmountWithDiscount).setScale(3);

        BigDecimal systemProfit = amountWithDiscounts.multiply(new BigDecimal("0.05")).setScale(3);;
        BigDecimal shopAAmountReal = shopAAmountWithDiscount.multiply(new BigDecimal("0.95")).setScale(3);;
        BigDecimal shopC2AmountReal = shopC2AmountWithDiscount.multiply(new BigDecimal("0.95")).setScale(3);;
        BigDecimal shopC3AmountReal = shopC3AmountWithDiscount.multiply(new BigDecimal("0.95")).setScale(3);;
        //calculate ammounts - end

        Assert.assertEquals(amountWithDiscounts, orderOperations.getFinalPrice(order));
        Assert.assertEquals(amountWithoutDiscounts.subtract(amountWithDiscounts), orderOperations.getDiscountSum(order));

        Assert.assertEquals(amountWithDiscounts, transactionOperations.getBuyerTransactionsAmmount(buyer));
        Assert.assertNull(transactionOperations.getShopTransactionsAmmount(shopA));
        Assert.assertNull(transactionOperations.getShopTransactionsAmmount(shopC2));
        Assert.assertNull(transactionOperations.getShopTransactionsAmmount(shopC3));
        Assert.assertEquals(new BigDecimal("0"), transactionOperations.getSystemProfit());

        generalOperations.time(2);
        Assert.assertEquals(initialTime, orderOperations.getSentTime(order));
        Assert.assertNull(orderOperations.getRecievedTime(order));
        Assert.assertEquals(orderOperations.getLocation(order), cityA);

        generalOperations.time(9);
        Assert.assertEquals(orderOperations.getLocation(order), cityA);

        generalOperations.time(8);
        Assert.assertEquals(orderOperations.getLocation(order), cityC5);

        generalOperations.time(5);
        Assert.assertEquals(orderOperations.getLocation(order), cityB);
        Assert.assertEquals(receivedTime, orderOperations.getRecievedTime(order));

        Assert.assertEquals(shopAAmountReal, transactionOperations.getShopTransactionsAmmount(shopA));
        Assert.assertEquals(shopC2AmountReal, transactionOperations.getShopTransactionsAmmount(shopC2));
        Assert.assertEquals(shopC3AmountReal, transactionOperations.getShopTransactionsAmmount(shopC3));
        Assert.assertEquals(systemProfit, transactionOperations.getSystemProfit());

        int shopATransactionId = transactionOperations.getTransactionForShopAndOrder(order, shopA);
        Assert.assertNotEquals(-1, shopATransactionId);
        Assert.assertEquals(receivedTime, transactionOperations.getTimeOfExecution(shopATransactionId));

    }

}
