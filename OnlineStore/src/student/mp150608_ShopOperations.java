package student;

import operations.ShopOperations;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class mp150608_ShopOperations implements ShopOperations {
    @Override
    public int createShop(String name, String cityName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            int cityId;
            ps = c.prepareStatement("select * from CITY where NAME = ?");
            ps.setString(1, cityName);
            rs = ps.executeQuery();
            if (!rs.next()) {
                c.rollback();
                throw new RuntimeException("City with provided name doesn't exist.");
            }
            else cityId = rs.getInt("ID");

            c.prepareStatement("select * from SHOP where NAME = ?");
            ps.setString(1, name);
            rs = ps.executeQuery();
            if (rs.next()) {
                c.rollback();
                throw new RuntimeException("Shop with provided name already exists.");
            }

            ps = c.prepareStatement("insert into SHOP values(?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setInt(2, 0);
            ps.setInt(3, cityId);
            ps.setInt(4, 0);
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
                if (rs != null) rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public int setCity(int shopId, String cityName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            int cityId;
            ps = c.prepareStatement("select * from CITY where NAME = ?");
            ps.setString(1, cityName);
            rs = ps.executeQuery();
            if (!rs.next()) {
                c.rollback();
                throw new RuntimeException("City with provided name doesn't exist.");
            }
            else cityId = rs.getInt("ID");

            ps = c.prepareStatement("update SHOP set CITY_ID = ? where ID = ?");
            ps.setInt(1, cityId);
            ps.setInt(2, shopId);
            if (ps.executeUpdate() == 0) {
                c.rollback();
                throw new RuntimeException("Execute update returned 0 updated rows.");
            }
            c.commit();
            c.setAutoCommit(true);
            return 1;
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public int getCity(int shopId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from SHOP where ID = ?");
            ps.setInt(1, shopId);
            rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("SHOP_ID");
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public int setDiscount(int shopId, int discountPercentage) {
        PreparedStatement ps = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("update SHOP set DISCOUNT = ? where ID = ?");
            ps.setInt(1, discountPercentage);
            ps.setInt(2, shopId);
            if (ps.executeUpdate() == 0) {
                c.rollback();
                throw new RuntimeException("Execute update returned 0 updated rows.");
            }
            c.commit();
            c.setAutoCommit(true);
            return 1;
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

    @Override
    public int increaseArticleCount(int articleId, int increment) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            int quantity;
             ps = c.prepareStatement("select * from ARTICLE where ID = ?");
            ps.setInt(1, articleId);
            rs = ps.executeQuery();
            if (!rs.next()) {
                c.rollback();
                throw new RuntimeException("Provided article doesn't exist.");
            }
            else quantity = rs.getInt("QUANTITY");
            ps = c.prepareStatement("update ARTICLE set QUANTITY = ? where ID = ?");
            ps.setInt(1, quantity + increment);
            ps.setInt(2, articleId);
            if (ps.executeUpdate() == 0) {
                c.rollback();
                throw new RuntimeException("Execute update returned 0 updated rows.");
            }
            c.commit();
            c.setAutoCommit(true);
            return quantity + increment;
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public int getArticleCount(int articleId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from ARTICLE where ID = ?");
            ps.setInt(1, articleId);
            rs = ps.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException("Provided article doesn't exist.");
            }
            else return rs.getInt("QUANTITY");
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public List<Integer> getArticles(int shopId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from ARTICLE where SHOP_ID = ?");
            ps.setInt(1, shopId);
            rs = ps.executeQuery();
            List<Integer> articles = new LinkedList<>();
            while(rs.next()){
                articles.add(rs.getInt("ID"));
            }
            return articles;
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int getDiscount(int shopId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from SHOP where ID = ?");
            ps.setInt(1, shopId);
            rs = ps.executeQuery();
            if (!rs.next()) {
                c.rollback();
                throw new RuntimeException("Provided shop doesn't exist.");
            }
            else return rs.getInt("DISCOUNT");
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }
}
