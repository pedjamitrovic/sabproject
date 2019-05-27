package student;

import operations.OrderOperations;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;

public class mp150608_OrderOperations implements OrderOperations {
    @Override
    public int addArticle(int orderId, int articleId, int count) {
        return 0;
    }

    @Override
    public int removeArticle(int orderId, int articleId) {
        return 0;
    }

    @Override
    public List<Integer> getItems(int orderId) {
        return null;
    }

    @Override
    public int completeOrder(int orderId) {
        return 0;
    }

    @Override
    public BigDecimal getFinalPrice(int orderId) {
        return null;
    }

    @Override
    public BigDecimal getDiscountSum(int orderId) {
        return null;
    }

    @Override
    public String getState(int orderId) {
        return null;
    }

    @Override
    public Calendar getSentTime(int orderId) {
        return null;
    }

    @Override
    public Calendar getRecievedTime(int orderId) {
        return null;
    }

    @Override
    public int getBuyer(int orderId) {
        return 0;
    }

    @Override
    public int getLocation(int orderId) {
        return 0;
    }
}
