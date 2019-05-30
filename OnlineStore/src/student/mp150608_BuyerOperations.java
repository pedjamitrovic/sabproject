package student;

import operations.BuyerOperations;

import java.math.BigDecimal;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class mp150608_BuyerOperations implements BuyerOperations {
    @Override
    public int createBuyer(String name, int cityId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("insert into BUYER values(?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setInt(2, 0);
            ps.setInt(3, cityId);
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
    public int setCity(int buyerId, int cityId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("update BUYER set CITY_ID = ? where ID = ?");
            ps.setInt(1, cityId);
            ps.setInt(2, buyerId);
            if (ps.executeUpdate() == 0) return -1;
            else return 1;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int getCity(int buyerId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            int quantity = -1;
            PreparedStatement ps = c.prepareStatement("select * from BUYER where ID = ?");
            ps.setInt(1, buyerId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("CITY_ID");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public BigDecimal increaseCredit(int buyerId, BigDecimal credit) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            BigDecimal previousCredit = new BigDecimal(0);
            PreparedStatement ps = c.prepareStatement("select * from BUYER where ID = ?");
            ps.setInt(1, buyerId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            else previousCredit = rs.getBigDecimal("CREDIT");

            ps = c.prepareStatement("update BUYER set CREDIT = ? where ID = ?");
            ps.setBigDecimal(1, credit.add(previousCredit));
            ps.setInt(2, buyerId);
            if (ps.executeUpdate() == 0) return null;
            else return credit;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int createOrder(int buyerId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("insert into ORDER values(?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, "created");
            ps.setDate(2, Date.valueOf("01/01/2000"));
            ps.setInt(3, buyerId);
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
    public List<Integer> getOrders(int buyerId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from ORDER where BUYER_ID = ?");
            ps.setInt(1, buyerId);
            ResultSet rs = ps.executeQuery();
            List<Integer> orders = new LinkedList<>();
            while(rs.next()){
                orders.add(rs.getInt("ID"));
            }
            return orders;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getCredit(int buyerId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from BUYER where ID = ?");
            ps.setInt(1, buyerId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            else return rs.getBigDecimal("CREDIT");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
