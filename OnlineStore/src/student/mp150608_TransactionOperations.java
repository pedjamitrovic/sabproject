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
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            boolean atLeastOneTransactionFound = false;
            ps = c.prepareStatement("select * from [TRANSACTION] where SENDER = ? and [TYPE] = ?");
            ps.setInt(1, buyerId);
            ps.setInt(2, 1);
            rs = ps.executeQuery();
            BigDecimal sum = new BigDecimal(0).setScale(3);
            while(rs.next()){
                atLeastOneTransactionFound = true;
                sum = sum.add(rs.getBigDecimal("AMOUNT").setScale(3));
            }
            if (atLeastOneTransactionFound) return sum;
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
    public BigDecimal getShopTransactionsAmmount(int shopId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            boolean atLeastOneTransactionFound = false;
            ps = c.prepareStatement("select * from [TRANSACTION] where RECEIVER = ? and [TYPE] = ?");
            ps.setInt(1, shopId);
            ps.setInt(2, 2);
            rs = ps.executeQuery();
            BigDecimal sum = new BigDecimal(0).setScale(3);
            while(rs.next()){
                atLeastOneTransactionFound = true;
                sum = sum.add(rs.getBigDecimal("AMOUNT").setScale(3));
            }
            if (atLeastOneTransactionFound) return sum;
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
    public List<Integer> getTransationsForBuyer(int buyerId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [TRANSACTION] where SENDER = ? and [TYPE] = ?");
            ps.setInt(1, buyerId);
            ps.setInt(2, 1);
            rs = ps.executeQuery();
            LinkedList<Integer> transactions = new LinkedList<>();
            while(rs.next()){
                transactions.add(rs.getInt("ID"));
            }
            if (transactions.size() > 0) return transactions;
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
    public int getTransactionForBuyersOrder(int orderId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [TRANSACTION] where ORDER_ID = ? and [TYPE] = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, 1);
            rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("ID");
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
    public int getTransactionForShopAndOrder(int orderId, int shopId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [TRANSACTION] where ORDER_ID = ? and RECEIVER = ? and [TYPE] = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, shopId);
            ps.setInt(3, 2);
            rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("ID");
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
    public List<Integer> getTransationsForShop(int shopId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [TRANSACTION] where RECEIVER = ? and [TYPE] = ?");
            ps.setInt(1, shopId);
            ps.setInt(2, 2);
            rs = ps.executeQuery();
            LinkedList<Integer> transactions = new LinkedList<>();
            while(rs.next()){
                transactions.add(rs.getInt("ID"));
            }
            if (transactions.size() > 0) return transactions;
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
    public Calendar getTimeOfExecution(int transactionId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [TRANSACTION] where ID = ?");
            ps.setInt(1, transactionId);
            rs = ps.executeQuery();
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
    public BigDecimal getAmmountThatBuyerPayedForOrder(int orderId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [TRANSACTION] where ORDER_ID = ? and [TYPE] = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, 1);
            rs = ps.executeQuery();
            if (!rs.next()) return null;
            else return rs.getBigDecimal("AMOUNT").setScale(3);
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
    public BigDecimal getAmmountThatShopRecievedForOrder(int shopId, int orderId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            boolean atLeastOneTransactionFound = false;
            ps = c.prepareStatement("select * from [TRANSACTION] where ORDER_ID = ? and RECEIVER = ? and [TYPE] = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, shopId);
            ps.setInt(3, 2);
            rs = ps.executeQuery();
            BigDecimal sum = new BigDecimal(0).setScale(3);
            while(rs.next()){
                atLeastOneTransactionFound = true;
                sum.add(rs.getBigDecimal("AMOUNT").setScale(3));
            }
            if(atLeastOneTransactionFound) return sum;
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
    public BigDecimal getTransactionAmount(int transactionId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [TRANSACTION] where ID = ?");
            ps.setInt(1, transactionId);
            rs = ps.executeQuery();
            if (!rs.next()) return null;
            else return rs.getBigDecimal("AMOUNT").setScale(3);
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
    public BigDecimal getSystemProfit() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        CallableStatement cs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [ORDER] where STATE = ?");
            ps.setString(1,"arrived");
            rs = ps.executeQuery();
            cs = c.prepareCall("{call SP_SYSTEM_TRANSACTIONS(?, ?, ?)}");
            cs.registerOutParameter(2, Types.DECIMAL);
            cs.registerOutParameter(3, Types.DECIMAL);
            BigDecimal profit =  new BigDecimal(0).setScale(3);
            while(rs.next()){
                cs.setInt(1, rs.getInt("ID"));
                cs.execute();
                profit = profit.add(cs.getBigDecimal(3).setScale(3));
            }
            return profit;
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
                if (cs != null) cs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
