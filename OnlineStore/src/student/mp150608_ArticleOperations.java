package student;

import operations.ArticleOperations;

import java.sql.*;

public class mp150608_ArticleOperations implements ArticleOperations {
    @Override
    public int createArticle(int shopId, String articleName, int articlePrice) {
        PreparedStatement ps = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("insert into ARTICLE values(?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, articleName);
            ps.setInt(2, articlePrice);
            ps.setFloat(3, 0);
            ps.setInt(4, shopId);
            if (ps.executeUpdate() == 0) {
                c.rollback();
                throw new RuntimeException("Execute update returned 0 updated rows.");
            }
            try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    c.commit();
                    c.setAutoCommit(true);
                    return generatedKeys.getInt(1);
                }
                else {
                    c.rollback();
                    throw new RuntimeException("Generated keys error.");
                }
            }
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }
}
