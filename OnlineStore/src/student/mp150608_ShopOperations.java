package student;

import operations.ShopOperations;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class mp150608_ShopOperations implements ShopOperations {
    @Override
    public int createShop(String name, String cityName) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            int cityId = -1;
            PreparedStatement ps = c.prepareStatement("select * from CITY where NAME = ?");
            ps.setString(1, cityName);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else cityId = rs.getInt("ID");

            c.prepareStatement("select * from SHOP where NAME = ?");
            ps.setString(1, name);
            rs = ps.executeQuery();
            if (rs.next()) return -1;

            ps = c.prepareStatement("insert into SHOP values(?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setInt(2, 0);
            ps.setInt(3, cityId);
            ps.setInt(4, 0);
            if (ps.executeUpdate() == 0) return -1;
            try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                if (generatedKeys.next()) return generatedKeys.getInt(1);
                else return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int setCity(int shopId, String cityName) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            int cityId = -1;
            PreparedStatement ps = c.prepareStatement("select * from CITY where NAME = ?");
            ps.setString(1, cityName);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else cityId = rs.getInt("ID");

            ps = c.prepareStatement("update SHOP set CITY_ID = ? where ID = ?");
            ps.setInt(1, cityId);
            ps.setInt(2, shopId);
            if (ps.executeUpdate() == 0) return -1;
            else return 1;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int getCity(int shopId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from SHOP where ID = ?");
            ps.setInt(1, shopId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("SHOP_ID");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int setDiscount(int shopId, int discountPercentage) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("update SHOP set DISCOUNT = ? where ID = ?");
            ps.setInt(1, discountPercentage);
            ps.setInt(2, shopId);
            if (ps.executeUpdate() == 0) return -1;
            else return 1;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int increaseArticleCount(int articleId, int increment) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            int quantity = -1;
            PreparedStatement ps = c.prepareStatement("select * from ARTICLE where ID = ?");
            ps.setInt(1, articleId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else quantity = rs.getInt("QUANTITY");

            ps = c.prepareStatement("update ARTICLE set QUANTITY = ? where ID = ?");
            ps.setInt(1, quantity + increment);
            ps.setInt(2, articleId);
            if (ps.executeUpdate() == 0) return -1;
            else return quantity + increment;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int getArticleCount(int shopId, int articleId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            int quantity = -1;
            PreparedStatement ps = c.prepareStatement("select * from ARTICLE where ID = ? AND SHOP_ID = ?");
            ps.setInt(1, articleId);
            ps.setInt(2, shopId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("QUANTITY");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Integer> getArticles(int shopId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from ARTICLE where SHOP_ID = ?");
            ps.setInt(1, shopId);
            ResultSet rs = ps.executeQuery();
            List<Integer> articles = new LinkedList<>();
            while(rs.next()){
                articles.add(rs.getInt("ID"));
            }
            return articles;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int getDiscount(int shopId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            int quantity = -1;
            PreparedStatement ps = c.prepareStatement("select * from SHOP where ID = ?");
            ps.setInt(1, shopId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("DISCOUNT");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
