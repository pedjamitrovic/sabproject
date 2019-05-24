package operations;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;

public interface TransactionOperations {

    /**
     * Gets sum of all transactions amounts for buyer
     * @param buyerId buyer's id
     * @return sum of all transactions, 0 if there are not transactions, -1 if failure
     */
    BigDecimal getBuyerTransactionsAmmount(int buyerId);

    /**
     * Gets sum of all transactions amounts for shop
     * @param shopId shop's id
     * @return sum of all transactions, 0 if there are not transactions, -1 if failure
     */
    BigDecimal getShopTransactionsAmmount(int shopId);

    /**
     * Gets all transactions for buyer
     * @param buyerId buyer id
     * @return list of transations ids, null if failure
     */
    List<Integer> getTransationsForBuyer(int buyerId);

    /**
     * Gets transaction that buyer made for paying an order.
     * @param orderId order's id
     * @return transaction's id, -1 if failure
     */
    int getTransactionForBuyersOrder(int orderId);

    /**
     * Gets transaction for recieved order that system made to shop.
     * @param orderId order's id
     * @param shopId shop's id
     * @return transaction's id, -1 if failure
     */
    int getTransactionForShopAndOrder(int orderId, int shopId);

    /**
     * Gets all transactions for shop
     * @param shopId buyer id
     * @return list of transations ids, null if failure
     */
    List<Integer> getTransationsForShop(int shopId);

    /**
     * get transaction's execution time. Execution time must be equal to order's recieve time.
     * @param transactionId transaction's id
     * @return time of execution, null if payment is not done or if failure
     */
    Calendar getTimeOfExecution(int transactionId);


    /**
     * Gets sum that buyer payed for an order
     * @param orderId order's id
     * @return ammount buyer payed for an order
     */
    BigDecimal getAmmountThatBuyerPayedForOrder(int orderId);

    /**
     * Gets sum that shop recieved for an order
     * @param shopId shop's id
     * @param orderId order's id
     * @return ammount shop recieved for an order
     */
    BigDecimal getAmmountThatShopRecievedForOrder(int shopId, int orderId);

    /**
     * Gets transaction's amount.
     * @param transactionId transaction's id
     * @return ammount that is transferd via transaction
     */
    BigDecimal getTransactionAmount(int transactionId);

    /**
     * Gets system profit. System profit calculation is based only on arrived orders.
     * @return system profit.
     */
    BigDecimal getSystemProfit();
}
