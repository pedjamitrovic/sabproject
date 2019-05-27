package student;

import operations.TransactionOperations;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;

public class mp150608_TransactionOperations implements TransactionOperations {
    @Override
    public BigDecimal getBuyerTransactionsAmmount(int buyerId) {
        return null;
    }

    @Override
    public BigDecimal getShopTransactionsAmmount(int shopId) {
        return null;
    }

    @Override
    public List<Integer> getTransationsForBuyer(int buyerId) {
        return null;
    }

    @Override
    public int getTransactionForBuyersOrder(int orderId) {
        return 0;
    }

    @Override
    public int getTransactionForShopAndOrder(int orderId, int shopId) {
        return 0;
    }

    @Override
    public List<Integer> getTransationsForShop(int shopId) {
        return null;
    }

    @Override
    public Calendar getTimeOfExecution(int transactionId) {
        return null;
    }

    @Override
    public BigDecimal getAmmountThatBuyerPayedForOrder(int orderId) {
        return null;
    }

    @Override
    public BigDecimal getAmmountThatShopRecievedForOrder(int shopId, int orderId) {
        return null;
    }

    @Override
    public BigDecimal getTransactionAmount(int transactionId) {
        return null;
    }

    @Override
    public BigDecimal getSystemProfit() {
        return null;
    }
}
