package student;

import operations.OrderOperations;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

public class mp150608_OrderOperations implements OrderOperations {
    @Override
    public int addArticle(int orderId, int articleId, int count) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            int articleQuantity = -1;
            PreparedStatement ps = c.prepareStatement("select * from ARTICLE where ID = ?");
            ps.setInt(1, articleId);
            ResultSet rsArticle = ps.executeQuery();
            if (!rsArticle.next()) return -1;
            else{
                articleQuantity = rsArticle.getInt("QUANTITY");
                if(articleQuantity < count) return -1;
            }
            ps = c.prepareStatement("select * from ORDER_ITEM where ORDER_ID = ? and ARTICLE_ID = ?", ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, orderId);
            ps.setInt(2, articleId);
            ResultSet rsOrderItem = ps.executeQuery();
            if(!rsOrderItem.next()){
                String[] generatedColumns = { "ID" };
                ps = c.prepareStatement("insert into ORDER_ITEM values(?,?,?)", generatedColumns);
                ps.setInt(1, orderId);
                ps.setInt(2, articleId);
                ps.setInt(3, count);
                if (ps.executeUpdate() == 0) return -1;
                rsArticle.updateInt("QUANTITY", articleQuantity - count);
                rsArticle.updateRow();
                try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                    if (generatedKeys.next()) return generatedKeys.getInt("ID");
                    else return -1;
                }
            }
            else{
                rsOrderItem.updateInt("QUANTITY", articleQuantity + count);
                rsOrderItem.updateRow();
                rsArticle.updateInt("QUANTITY", articleQuantity - count);
                rsArticle.updateRow();
                return rsOrderItem.getInt("ID");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int removeArticle(int orderId, int articleId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("delete from ORDER_ITEM where ORDER_ID = ? AND ARTICLE_ID = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, articleId);
            if (ps.executeUpdate() == 0) return -1;
            else return 1;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Integer> getItems(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from ORDER_ITEM where ORDER_ID = ?");
            ps.setInt(1, orderId);
            ResultSet rs = ps.executeQuery();
            List<Integer> items = new LinkedList<>();
            while(rs.next()){
                items.add(rs.getInt("ID"));
            }
            return items;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int completeOrder(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("update ORDER set STATE = ? where ID = ?");
            ps.setString(1, "sent");
            ps.setInt(2, orderId);
            if (ps.executeUpdate() == 0) return -1;
            else return 1;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public BigDecimal getFinalPrice(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            CallableStatement cs = c.prepareCall("{call SP_FINAL_PRICE(?, ?)}");
            cs.setInt(1, orderId);
            cs.registerOutParameter(2, Types.DECIMAL);
            cs.execute();
            return cs.getBigDecimal(2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public BigDecimal getDiscountSum(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            CallableStatement cs = c.prepareCall("{call SP_DISCOUNT_SUM(?, ?)}");
            cs.setInt(1, orderId);
            cs.registerOutParameter(2, Types.DECIMAL);
            cs.execute();
            return cs.getBigDecimal(2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getState(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from ORDER where ID = ?");
            ps.setInt(1, orderId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            else return rs.getString("STATE");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Calendar getSentTime(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [ORDER] where ID = ?");
            ps.setInt(1, orderId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            else {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTime(rs.getDate("SENT_TIME"));
                return calendar;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Calendar getRecievedTime(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [ORDER] where ID = ?");
            ps.setInt(1, orderId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            else {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTime(rs.getDate("RECIEVED_TIME"));
                return calendar;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int getBuyer(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [ORDER] where ID = ?");
            ps.setInt(1, orderId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("BUYER");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public int getLocation(int orderId) {
        return 0;
    }
}
