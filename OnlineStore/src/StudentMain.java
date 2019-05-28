import operations.*;
import org.junit.Test;
import student.*;
import tests.TestHandler;
import tests.TestRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class StudentMain {
    public static String connectionUrl = "jdbc:sqlserver://localhost:1433;databaseName=DomaciSAB;user=pedja;password=";

    public static void main(String[] args) {

        ArticleOperations articleOperations = new mp150608_ArticleOperations();
        BuyerOperations buyerOperations = new mp150608_BuyerOperations();
        CityOperations cityOperations = new mp150608_CityOperations();
        GeneralOperations generalOperations = new mp150608_GeneralOperations();
        OrderOperations orderOperations = new mp150608_OrderOperations();
        ShopOperations shopOperations = new mp150608_ShopOperations();
        TransactionOperations transactionOperations = new mp150608_TransactionOperations();

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
