package student;

import operations.ArticleOperations;

import java.sql.*;

public class mp150608_ArticleOperations implements ArticleOperations {
    @Override
    public int createArticle(int shopId, String articleName, int articlePrice) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from ARTICLE where NAME = ?");
            ps.setString(1, articleName);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return -1;

            String[] generatedColumns = { "ID" };
            ps = c.prepareStatement("insert into ARTICLE values(?, ?, ?, ?)", generatedColumns);
            ps.setString(1, articleName);
            ps.setInt(2, articlePrice);
            ps.setFloat(3, 0);
            ps.setInt(4, shopId);
            if (ps.executeUpdate() == 0) return -1;
            try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                if (generatedKeys.next()) return generatedKeys.getInt("ID");
                else return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
