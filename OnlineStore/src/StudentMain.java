import operations.*;
import org.junit.Test;
import tests.TestHandler;
import tests.TestRunner;

public class StudentMain {

    public static void main(String[] args) {

        ArticleOperations articleOperations = null; // Change this for your implementation (points will be negative if interfaces are not implemented).
        BuyerOperations buyerOperations = null;
        CityOperations cityOperations = null;
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

        TestRunner.runTests();
    }
}
