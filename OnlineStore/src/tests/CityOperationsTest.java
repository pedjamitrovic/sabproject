package tests;

import operations.BuyerOperations;
import operations.CityOperations;
import operations.GeneralOperations;
import operations.ShopOperations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

public class CityOperationsTest {

    private TestHandler testHandler;
    private GeneralOperations generalOperations;
    private CityOperations cityOperations;
    private ShopOperations shopOperations;


    @Before
    public void setUp() throws Exception {
        this.testHandler = TestHandler.getInstance();
        Assert.assertNotNull(this.testHandler);

        this.cityOperations = this.testHandler.getCityOperations();
        Assert.assertNotNull(this.cityOperations);

        generalOperations = testHandler.getGeneralOperations();
        Assert.assertNotNull(generalOperations);


        this.shopOperations = this.testHandler.getShopOperations();
        Assert.assertNotNull(this.shopOperations);

        generalOperations.eraseAll();
    }

    @After
    public void tearDown() throws Exception {
        generalOperations.eraseAll();
    }

    @Test
    public void createCity(){
        int cityVranje = cityOperations.createCity("Vranje");
        Assert.assertEquals(1, cityOperations.getCities().size());
        Assert.assertEquals(cityVranje, (int) cityOperations.getCities().get(0));
    }

    @Test
    public void insertShops(){
        int cityId = cityOperations.createCity("Vranje");

        int shopId1 = shopOperations.createShop("Gigatron", "Vranje");
        int shopId2 = shopOperations.createShop("Teranova", "Vranje");

        List<Integer> shops = cityOperations.getShops(cityId);
        Assert.assertEquals(2, shops.size());
        Assert.assertTrue(shops.contains(shopId1) && shops.contains(shopId2));
    }

    @Test
    public void connectCities(){
        int cityVranje = cityOperations.createCity("Vranje");
        int cityLeskovac = cityOperations.createCity("Leskovac");
        int cityNis = cityOperations.createCity("Nis");

        Assert.assertNotEquals(-1, cityLeskovac);
        Assert.assertNotEquals(-1, cityVranje);
        Assert.assertNotEquals(-1, cityNis);

        cityOperations.connectCities(cityNis, cityVranje, 50);
        cityOperations.connectCities(cityVranje, cityLeskovac, 70);

        List<Integer> connectedCities = cityOperations.getConnectedCities(cityVranje);
        Assert.assertEquals(2, connectedCities.size());
        Assert.assertTrue(connectedCities.contains(cityLeskovac) && connectedCities.contains(cityNis));
    }

}
