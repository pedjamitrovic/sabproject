package operations;

public interface ArticleOperations {

    /**
     * Creates new article in shop with count 0.
     * @param shopId shop id
     * @param articleName article name
     * @param articlePrice price of the article
     * @return id of the article, -1 in failure
     */
    int createArticle(int shopId, String articleName, int articlePrice);

}
