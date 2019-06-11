package student;

import operations.BuyerOperations;

import java.math.BigDecimal;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class mp150608_BuyerOperations implements BuyerOperations {
    @Override
    public int createBuyer(String name, int cityId) {
        PreparedStatement ps = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("insert into BUYER values(?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setInt(2, 0);
            ps.setInt(3, cityId);
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

    @Override
    public int setCity(int buyerId, int cityId) {
        PreparedStatement ps = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("update BUYER set CITY_ID = ? where ID = ?");
            ps.setInt(1, cityId);
            ps.setInt(2, buyerId);
            if (ps.executeUpdate() == 0) {
                c.rollback();
                throw new RuntimeException("Execute update returned 0 updated rows.");
            }
            else {
                c.commit();
                c.setAutoCommit(true);
                return 1;
            }
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
            } catch (SQLException | RuntimeException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public int getCity(int buyerId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from BUYER where ID = ?");
            ps.setInt(1, buyerId);
            rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("CITY_ID");
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException | RuntimeException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public BigDecimal increaseCredit(int buyerId, BigDecimal credit) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            BigDecimal previousCredit;
            ps = c.prepareStatement("select * from [BUYER] where ID = ?");
            ps.setInt(1, buyerId);
            rs = ps.executeQuery();
            if (!rs.next()) {
                c.rollback();
                throw new RuntimeException("Buyer is not valid.");
            }
            else previousCredit = rs.getBigDecimal("CREDIT");

            ps = c.prepareStatement("update [BUYER] set CREDIT = ? where ID = ?");
            ps.setBigDecimal(1, credit.add(previousCredit));
            ps.setInt(2, buyerId);
            if (ps.executeUpdate() == 0) {
                c.rollback();
                throw new RuntimeException("Execute update returned 0 updated rows.");
            }
            else {
                c.commit();
                c.setAutoCommit(true);
                return credit;
            }
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException | RuntimeException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int createOrder(int buyerId) {
        PreparedStatement ps = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("insert into [ORDER] values(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, "created");
            ps.setDate(2, null);
            ps.setDate(3, null);
            ps.setDate(4, null);
            ps.setInt(5, buyerId);
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
            } catch (SQLException | RuntimeException e) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public List<Integer> getOrders(int buyerId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [ORDER] where BUYER_ID = ?");
            ps.setInt(1, buyerId);
            rs = ps.executeQuery();
            List<Integer> orders = new LinkedList<>();
            while(rs.next()){
                orders.add(rs.getInt("ID"));
            }
            return orders;
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException | RuntimeException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public BigDecimal getCredit(int buyerId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from BUYER where ID = ?");
            ps.setInt(1, buyerId);
            rs = ps.executeQuery();
            if (!rs.next()) return null;
            else return rs.getBigDecimal("CREDIT");
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException | RuntimeException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
