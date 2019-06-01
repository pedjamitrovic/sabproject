package student;

import operations.TransactionOperations;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

public class mp150608_TransactionOperations implements TransactionOperations {
    @Override
    public BigDecimal getBuyerTransactionsAmmount(int buyerId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where SENDER = ? and TYPE = ?");
            ps.setInt(1, buyerId);
            ps.setInt(2, 1);
            ResultSet rs = ps.executeQuery();
            BigDecimal sum = new BigDecimal(0);
            while(rs.next()){
                sum.add(rs.getBigDecimal("AMOUNT"));
            }
            return sum;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getShopTransactionsAmmount(int shopId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where RECEIVER = ? and TYPE = ?");
            ps.setInt(1, shopId);
            ps.setInt(2, 2);
            ResultSet rs = ps.executeQuery();
            BigDecimal sum = new BigDecimal(0);
            while(rs.next()){
                sum.add(rs.getBigDecimal("AMOUNT"));
            }
            return sum;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Integer> getTransationsForBuyer(int buyerId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where SENDER = ? and TYPE = ?");
            ps.setInt(1, buyerId);
            ps.setInt(2, 1);
            ResultSet rs = ps.executeQuery();
            LinkedList<Integer> transactions = new LinkedList<>();
            while(rs.next()){
                transactions.add(rs.getInt("ID"));
            }
            return transactions;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int getTransactionForBuyersOrder(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where ORDER_ID = ? and TYPE = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, 1);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("ID");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int getTransactionForShopAndOrder(int orderId, int shopId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where ORDER_ID = ? and RECEIVER = ? and TYPE = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, shopId);
            ps.setInt(3, 2);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("ID");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Integer> getTransationsForShop(int shopId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where and RECEIVER = ? and TYPE = ?");
            ps.setInt(1, shopId);
            ps.setInt(2, 2);
            ResultSet rs = ps.executeQuery();
            LinkedList<Integer> transactions = new LinkedList<>();
            while(rs.next()){
                transactions.add(rs.getInt("ID"));
            }
            return transactions;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Calendar getTimeOfExecution(int transactionId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where ID = ?");
            ps.setInt(1, transactionId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            else {
                Date execution_time = rs.getDate("EXECUTION_TIME");
                if(execution_time != null) {
                    GregorianCalendar calendar = new GregorianCalendar();
                    calendar.setTime(execution_time);
                    return calendar;
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getAmmountThatBuyerPayedForOrder(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where ORDER_ID = ? and TYPE = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, 1);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            else return rs.getBigDecimal("AMOUNT");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getAmmountThatShopRecievedForOrder(int shopId, int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where ORDER_ID = ? and RECEIVER = ? and TYPE = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, shopId);
            ps.setInt(3, 2);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            LinkedList<Integer> transactions = new LinkedList<>();
            while(rs.next()){
                transactions.add(rs.getInt("ID"));
            }
            BigDecimal sum = new BigDecimal(0);
            while(rs.next()){
                sum.add(rs.getBigDecimal("AMOUNT"));
            }
            return sum;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getTransactionAmount(int transactionId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [TRANSACTION] where ID = ?");
            ps.setInt(1, transactionId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            else return rs.getBigDecimal("AMOUNT");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getSystemProfit() {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            CallableStatement cs = c.prepareCall("{? = call CALC_SYSTEM_PROFIT()}");
            cs.registerOutParameter(1, Types.DECIMAL);
            cs.execute();
            return cs.getBigDecimal(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
