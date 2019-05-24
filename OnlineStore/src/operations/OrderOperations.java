package operations;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;

public interface OrderOperations {

    /**
     * Adds article to order. It adds articles only if there are enough of them in shop. If article is in order already, it only increases count.
     * @param orderId order id
     * @param articleId article id
     * @param count number of articles to be added
     * @return item id (item contains information about number of article instances in particular order), -1 if failure
     */
    int addArticle(int orderId, int articleId, int count);

    /**
     * Removes article from order.
     * @param orderId order id
     * @param articleId article id
     * @return 1 if success, -1 if failure
     */
    int removeArticle(int orderId, int articleId);

    /**
     * Get all items for order.
     * @param orderId order's id
     * @return list of item ids for an order
     */
    List<Integer> getItems(int orderId);

    /**
     * Sends order to the system. Order will be immediately sent.
     * @param orderId oreder id
     * @return 1 if success, -1 if failure
     */
    int completeOrder(int orderId);

    /**
     * Gets calculated final price after all discounts.
     * @param orderId order id
     * @return final price. Sum that buyer have to pay. -1 if failure or if order is not completed
     */
    BigDecimal getFinalPrice(int orderId);

    /**
     * Gets calculated discount for the order
     * @param orderId order id
     * @return total discount, -1 if failure or if order is not completed
     */
    float getDiscountSum(int orderId);

    /**
     * Gets state of the order.
     * @param orderId order's id
     * @return state of the order
     */
    String getState(int orderId);

    /**
     * Gets order's sending time
     * @param orderId order's id
     * @return order's sending time, null if failure
     */
    Calendar getSentTime(int orderId);

    /**
     * Gets time when order arrived to buyer's city.
     * @param orderId order id
     * @return order's recieve time, null if failure
     */
    Calendar getRecievedTime(int orderId);

    /**
     * Gets buyer.
     * @param orderId order's id
     * @return buyer's id
     */
    int getBuyer(int orderId);

    /**
     * Gets location for an order.
     * If order is assembled and order is moving from city C1 to city C2 then location of an order is city C1.
     * If order is not yet assembled then location of the order is location of the shop (associated with order) that is closest to buyer's city.
     * If order is in state "created" then location is -1.
     * @param orderId order's id
     * @return id of city, -1 if failure
     */
    int getLocation(int orderId);
}
