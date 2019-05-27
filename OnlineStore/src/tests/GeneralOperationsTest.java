package tests;

import operations.CityOperations;
import operations.GeneralOperations;
import operations.ShopOperations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Time;
import java.util.Calendar;
import java.util.List;

public class GeneralOperationsTest {

    private TestHandler testHandler;
    private GeneralOperations generalOperations;


    @Before
    public void setUp() throws Exception {
        this.testHandler = TestHandler.getInstance();
        Assert.assertNotNull(this.testHandler);

        generalOperations = testHandler.getGeneralOperations();
        Assert.assertNotNull(generalOperations);

        generalOperations.eraseAll();
    }

    @After
    public void tearDown() throws Exception {
        generalOperations.eraseAll();
    }

    @Test
    public void general(){
        Calendar time = Calendar.getInstance();
        time.clear();
        time.set(2018, Calendar.JANUARY, 1);
        generalOperations.setInitialTime(time);

        Calendar currentTime = generalOperations.getCurrentTime();
        Assert.assertEquals(time, currentTime);

        generalOperations.time(40);
        currentTime = generalOperations.getCurrentTime();
        Calendar newTime = Calendar.getInstance();
        newTime.clear();
        newTime.set(2018, Calendar.FEBRUARY, 10);
        Assert.assertEquals(currentTime, newTime);
    }

}
