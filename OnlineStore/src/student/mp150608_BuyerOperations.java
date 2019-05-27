package student;

import operations.BuyerOperations;

import java.math.BigDecimal;
import java.util.List;

public class mp150608_BuyerOperations implements BuyerOperations {
    @Override
    public int createBuyer(String name, int cityId) {
        return 0;
    }

    @Override
    public int setCity(int buyerId, int cityId) {
        return 0;
    }

    @Override
    public int getCity(int buyerId) {
        return 0;
    }

    @Override
    public BigDecimal increaseCredit(int buyerId, BigDecimal credit) {
        return null;
    }

    @Override
    public int createOrder(int buyerId) {
        return 0;
    }

    @Override
    public List<Integer> getOrders(int buyerId) {
        return null;
    }

    @Override
    public BigDecimal getCredit(int buyerId) {
        return null;
    }
}
