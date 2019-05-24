package operations;

import java.math.BigDecimal;
import java.util.List;

public interface BuyerOperations {

    /**
     * Creates new buyer with 0 credit.
     * @param name name of the buyer
     * @param cityId id of the city
     * @return buyer's id, or -1 if failure
     */
    int createBuyer(String name, int cityId);

    /**
     * Changes city for buyer.
     * @param buyerId id of the buyer
     * @param cityId id of the city
     * @return 1 if success, -1 if failure
     */
    int setCity(int buyerId, int cityId);

    /**
     * Gets city for buyer.
     * @param buyerId buyer's id
     * @return city's id, -1 if failure
     */
    int getCity(int buyerId);


    /**
     * Increases buyer's credit.
     * @param buyerId id of the buyer
     * @param credit credit
     * @return credit after addition
     */
    BigDecimal increaseCredit(int buyerId, BigDecimal credit);

    /**
     * Creates empty order.
     * @param buyerId buyer id
     * @return id of the order, -1 in failure
     */
    int createOrder(int buyerId);

    /**
     * Gets all orders for buyer
     * @param buyerId buyer id
     * @return list of order's ids for buyer
     */
    List<Integer> getOrders(int buyerId);

    /**
     * Gets credit for buyer.
     * @param buyerId buyer's id
     * @return credit for buyer
     */
    BigDecimal getCredit(int buyerId);

//    /**
//     * Reurn all buyers.
//     * @return list of buyer ids
//     */
//    List<Integer> getAllBuyers();
}
