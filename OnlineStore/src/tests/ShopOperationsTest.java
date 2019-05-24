package tests;

import operations.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

public class ShopOperationsTest {

    private TestHandler testHandler;
    private GeneralOperations generalOperations;
    private ShopOperations shopOperations;
    private CityOperations cityOperations;
    private ArticleOperations articleOperations;


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

        generalOperations = testHandler.getGeneralOperations();
        Assert.assertNotNull(generalOperations);

        generalOperations.eraseAll();
    }

    @After
    public void tearDown() throws Exception {
        generalOperations.eraseAll();
    }

    @Test
    public void createShop(){
        int cityId = cityOperations.createCity("Kragujevac");
        Assert.assertNotEquals(-1, cityId);
        int shopId = shopOperations.createShop("Gigatron", "Kragujevac");
        Assert.assertEquals(shopId, (int) cityOperations.getShops(cityId).get(0));
    }

    @Test
    public void setCity(){
        cityOperations.createCity("Kragujevac");
        int shopId = shopOperations.createShop("Gigatron", "Kragujevac");
        int cityId2 = cityOperations.createCity("Subotica");

        shopOperations.setCity(shopId, "Subotica");
        Assert.assertEquals(shopId, (int) cityOperations.getShops(cityId2).get(0));
    }

    @Test
    public void discount(){
        cityOperations.createCity("Kragujevac");
        int shopId = shopOperations.createShop("Gigatron", "Kragujevac");
        shopOperations.setDiscount(shopId, 20);
        Assert.assertEquals(20, shopOperations.getDiscount(shopId));
    }

    @Test
    public void articles(){
        cityOperations.createCity("Kragujevac");
        int shopId = shopOperations.createShop("Gigatron", "Kragujevac");

        int articleId = articleOperations.createArticle(shopId, "Olovka", 10);
        Assert.assertNotEquals(-1, articleId);

        int articleId2 = articleOperations.createArticle(shopId, "Gumica", 5);
        Assert.assertNotEquals(-1, articleId2);

        shopOperations.increaseArticleCount(articleId, 5);
        shopOperations.increaseArticleCount(articleId, 2);
        int articleCount = shopOperations.getArticleCount(shopId, articleId);
        Assert.assertEquals(7, articleCount);

        List<Integer> articles = shopOperations.getArticles(shopId);
        Assert.assertEquals(2, articles.size());
        Assert.assertTrue(articles.contains(articleId) && articles.contains(articleId2));
    }

}
