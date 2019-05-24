package tests;

import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;


public final class TestRunner
{
    private static final int MAX_POINTS_ON_PUBLIC_TEST = 10;
    private static final Class[] UNIT_TEST_CLASSES = {
            BuyerOperationsTest.class, CityOperationsTest.class,
            GeneralOperationsTest.class, ShopOperationsTest.class
    };
//    private static final Class[] UNIT_TEST_CLASSES_PRIVATE = {
//            CourierRequestOperationTest.class, CourierOperationsTest.class };
//
    private static final Class[] MODULE_TEST_CLASSES = { PublicModuleTest.class };
//    private static final Class[] MODULE_TEST_CLASSES_PRIVATE = { PrivateModuleTest.class };
//
//
    private static double runUnitTestsPublic() {
        double numberOfSuccessfulCases = 0;
        double numberOfAllCases = 0;
        double points=0;
        JUnitCore jUnitCore = new JUnitCore();

        for (Class testClass : UNIT_TEST_CLASSES) {
            System.out.println("\n" + testClass.getName());

            Request request = Request.aClass(testClass);
            Result result = jUnitCore.run(request);

            System.out.println("Successful:" + (result.getRunCount() - result.getFailureCount()));
            System.out.println("All:" + (result.getRunCount()));

            numberOfAllCases = result.getRunCount();
            numberOfSuccessfulCases = result.getRunCount() - result.getFailureCount();

            points+=numberOfSuccessfulCases/numberOfAllCases;
        }


        return points;
    }

    private static double runModuleTestsPublic() {
        double numberOfSuccessfulCases = 0;
        double numberOfAllCases = 0;
        double points=0;
        JUnitCore jUnitCore = new JUnitCore();

        for (Class testClass : MODULE_TEST_CLASSES) {
            System.out.println("\n" + testClass.getName());

            Request request = Request.aClass(testClass);
            Result result = jUnitCore.run(request);

            System.out.println("Successful:" + (result.getRunCount() - result.getFailureCount()));
            System.out.println("All:" + (result.getRunCount()));

            numberOfAllCases = result.getRunCount();
            numberOfSuccessfulCases = result.getRunCount() - result.getFailureCount();
            points+=numberOfSuccessfulCases/numberOfAllCases;
        }


        return points;
    }

    private static double runPublic() {
        double res = 0;
        res += runUnitTestsPublic() / UNIT_TEST_CLASSES.length * 6;
        res += runModuleTestsPublic() / MODULE_TEST_CLASSES.length * 4;
        return res;
    }

//
//    private static double runUnitTestsPrivate() {
//        double numberOfSuccessfulCases = 0;
//        double numberOfAllCases = 0;
//        double points=0;
//        JUnitCore jUnitCore = new JUnitCore();
//
//        for (Class testClass : UNIT_TEST_CLASSES_PRIVATE) {
//            System.out.println("\n" + testClass.getName());
//
//            Request request = Request.aClass(testClass);
//            Result result = jUnitCore.run(request);
//
//            System.out.println("Successful:" + (result.getRunCount() - result.getFailureCount()));
//            System.out.println("All:" + (result.getRunCount()));
//
//            numberOfAllCases = result.getRunCount();
//            numberOfSuccessfulCases = result.getRunCount() - result.getFailureCount();
//
//            points+=numberOfSuccessfulCases/numberOfAllCases;
//        }
//
//
//        System.out.println("\n" + PackageOperationsTest.class.getName());
//
//        Request request = Request.aClass(PackageOperationsTest.class);
//        Result result = jUnitCore.run(request);
//
//        System.out.println("Successful:" + (result.getRunCount() - result.getFailureCount()));
//        System.out.println("All:" + (result.getRunCount()));
//
//        numberOfAllCases = result.getRunCount();
//        numberOfSuccessfulCases = result.getRunCount() - result.getFailureCount();
//
//        points+=(numberOfSuccessfulCases/numberOfAllCases)*2;
//
//        return points;
//    }
//
//    private static double runModuleTestsPrivate() {
//        double numberOfSuccessfulCases = 0;
//        double numberOfAllCases = 0;
//        double points=0;
//        JUnitCore jUnitCore = new JUnitCore();
//
//        for (Class testClass : MODULE_TEST_CLASSES_PRIVATE) {
//            System.out.println("\n" + testClass.getName());
//
//            Request request = Request.aClass(testClass);
//            Result result = jUnitCore.run(request);
//
//            System.out.println("Successful:" + (result.getRunCount() - result.getFailureCount()));
//            System.out.println("All:" + (result.getRunCount()));
//
//            numberOfAllCases = result.getRunCount();
//            numberOfSuccessfulCases = result.getRunCount() - result.getFailureCount();
//            points+=numberOfSuccessfulCases/numberOfAllCases;
//        }
//
//        return points*6;
//    }


//
//    private static double runPrivate() {
//        double res = 0;
//        res += runUnitTestsPrivate();
//        res += runModuleTestsPrivate();
//        return res;
//    }

    public static void runTests() {
        double resultsPublic = runPublic();
        System.out.println("Points won on public tests is: " + resultsPublic + " out of 10");

//        double resultsPrivate = runPrivate();
//        System.out.println("Points won on private tests is: " + resultsPrivate + " out of 10");

//        System.out.println("Total points won: " + (resultsPrivate + resultsPublic) + " out of 20");

    }
}
