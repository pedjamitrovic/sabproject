package student;

import operations.ShopOperations;

import java.util.List;

public class mp150608_ShopOperations implements ShopOperations {
    @Override
    public int createShop(String name, String cityName) {
        return 0;
    }

    @Override
    public int setCity(int shopId, String cityName) {
        return 0;
    }

    @Override
    public int getCity(int shopId) {
        return 0;
    }

    @Override
    public int setDiscount(int shopId, int discountPercentage) {
        return 0;
    }

    @Override
    public int increaseArticleCount(int articleId, int increment) {
        return 0;
    }

    @Override
    public int getArticleCount(int shopId, int articleId) {
        return 0;
    }

    @Override
    public List<Integer> getArticles(int shopId) {
        return null;
    }

    @Override
    public int getDiscount(int shopId) {
        return 0;
    }
}
