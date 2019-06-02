package operations;

import java.util.List;

public interface ShopOperations {

    /**
     * Creates new shop with 0% discount. Shops must have unique name.
     * @param name name of the shop
     * @param cityName name of the city
     * @return id of the shop, or -1 in failure
     */
    int createShop(String name, String cityName);

    /**
     * Changes city for shop.
     * @param shopId id of the shop
     * @param cityName name of the city
     * @return 1 on success, -1 on failure
     */
    int setCity(int shopId, String cityName);

    /**
     * Gets city's id
     * @param shopId city for shop
     * @return city's id
     */
    int getCity(int shopId);

    /**
     * Sets discount for shop.
     * @param shopId id of the shop
     * @param discountPercentage discount in percentege
     * @return 1 on success, -1 on failure
     */
    int setDiscount(int shopId, int discountPercentage);

    /**
     * Increases number of articles in the shop.
     * @param articleId id of the article
     * @param increment number of articles to be stored in shop
     * @return number of articles after storing, -1 in failure
     */
    int increaseArticleCount(int articleId, int increment);

    /**
     * Gets count og articles in shop.
     * @param articleId id of the article
     * @return number of articles in shop
     */
    int getArticleCount(int articleId);

    /**
     * Gets all articles.
     * @param shopId shop's id
     * @return gets all article's ids in shop
     */
    List<Integer> getArticles(int shopId);

    /**
     * Get discount for shop.
     * @param shopId shop's id
     * @return discount percentage
     */
    int getDiscount(int shopId);
}
