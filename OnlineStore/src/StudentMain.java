import operations.*;
import org.junit.Test;
import student.mp150608_CityOperations;
import tests.TestHandler;
import tests.TestRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class StudentMain {
    public static String connectionUrl = "jdbc:sqlserver://localhost:1433;databaseName=DomaciSAB;user=pedja;password=";

    public static void main(String[] args) {

        ArticleOperations articleOperations = null;
        BuyerOperations buyerOperations = null;
        CityOperations cityOperations = new mp150608_CityOperations();
        GeneralOperations generalOperations = null;
        OrderOperations orderOperations = null;
        ShopOperations shopOperations = null;
        TransactionOperations transactionOperations = null;

        TestHandler.createInstance(
                articleOperations,
                buyerOperations,
                cityOperations,
                generalOperations,
                orderOperations,
                shopOperations,
                transactionOperations
        );

        //TestRunner.runTests();
    }
}
