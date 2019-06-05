package student;

import operations.OrderOperations;
import student.dijkstra.Graph;
import student.dijkstra.Pair;
import student.dijkstra.Vertex;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class mp150608_OrderOperations implements OrderOperations {
    @Override
    public int addArticle(int orderId, int articleId, int count) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement psCheckOrderValidity = c.prepareStatement("select * from [ORDER] as O where O.ID = ? and O.STATE = ?");
            psCheckOrderValidity.setInt(1, orderId);
            psCheckOrderValidity.setString(2, "created");
            ResultSet rsCheckOrderValidity = psCheckOrderValidity.executeQuery();
            if (!rsCheckOrderValidity.next()) return -1;
            int articleQuantity = -1;
            PreparedStatement ps = c.prepareStatement("select * from ARTICLE where ID = ?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, articleId);
            ResultSet rsArticle = ps.executeQuery();
            if (!rsArticle.next()) return -1;
            else{
                articleQuantity = rsArticle.getInt("QUANTITY");
                if(articleQuantity < count) return -1;
            }
            ps = c.prepareStatement("select * from ORDER_ITEM where ORDER_ID = ? and ARTICLE_ID = ?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, orderId);
            ps.setInt(2, articleId);
            ResultSet rsOrderItem = ps.executeQuery();
            if(!rsOrderItem.next()){
                ps = c.prepareStatement("insert into ORDER_ITEM values(?,?,?)", Statement.RETURN_GENERATED_KEYS);
                ps.setInt(1, orderId);
                ps.setInt(2, articleId);
                ps.setInt(3, count);
                if (ps.executeUpdate() == 0) return -1;
                rsArticle.updateInt("QUANTITY", articleQuantity - count);
                rsArticle.updateRow();
                try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                    if (generatedKeys.next()) return generatedKeys.getInt(1);
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
            c.setAutoCommit(false);
            PreparedStatement ps = null;
            CallableStatement cs = c.prepareCall("{call SP_FINAL_PRICE(?, ?)}");
            cs.setInt(1, orderId);
            cs.registerOutParameter(2, Types.DECIMAL);
            cs.execute();
            BigDecimal orderPrice = cs.getBigDecimal(2);
            PreparedStatement psCheckBuyerCredit = c.prepareStatement("select * from BUYER as B join [ORDER] as O on B.ID = O.BUYER_ID where O.ID = ?");
            psCheckBuyerCredit.setInt(1, orderId);
            ResultSet rsCheckBuyerCredit = psCheckBuyerCredit.executeQuery();

            int buyerId = -1;
            if(!rsCheckBuyerCredit.next()) return -1;
            else {
                BigDecimal buyerCredit = rsCheckBuyerCredit.getBigDecimal("CREDIT");
                buyerId = rsCheckBuyerCredit.getInt("ID");
                if(buyerCredit.compareTo(orderPrice) < 0) return -1;

                ps = c.prepareStatement("update BUYER set CREDIT = ? where ID = ?");
                ps.setBigDecimal(1, buyerCredit.subtract(orderPrice));
                ps.setInt(2, buyerId);
                if (ps.executeUpdate() == 0) {
                    c.rollback();
                    return -1;
                }
            }
            ps = c.prepareStatement("select * from [SYSTEM]");
            ResultSet rs = ps.executeQuery();
            Date executionTime = null;
            if (rs.next()) executionTime = rs.getDate("CURRENT_DATE");

            createTravelPathsForOrderItems(c, orderId, rsCheckBuyerCredit.getInt("CITY_ID"));

            ps = c.prepareStatement("insert into [TRANSACTION] values(?, ?, ?, ?, ?, ?)");
            ps.setInt(1, 1);
            ps.setInt(2, buyerId);
            ps.setInt(3, -1);
            ps.setDate(4, executionTime);
            ps.setBigDecimal(5, orderPrice);
            ps.setInt(6, orderId);
            if (ps.executeUpdate() == 0) {
                c.rollback();
                return -1;
            }

            ps = c.prepareStatement("update [ORDER] set STATE = ?, SENT_TIME = ? where ID = ?");
            ps.setString(1, "sent");
            ps.setDate(2, executionTime);
            ps.setInt(3, orderId);
            if (ps.executeUpdate() == 0) {
                c.rollback();
                return -1;
            }

            c.commit();
            return 1;
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
            return cs.getBigDecimal(2).setScale(3);
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
            return cs.getBigDecimal(2).setScale(3);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getState(int orderId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [ORDER] where ID = ?");
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
                Date sentTime = rs.getDate("SENT_TIME");
                if (sentTime == null) return null;
                else {
                    GregorianCalendar calendar = new GregorianCalendar();
                    calendar.setTime(sentTime);
                    return calendar;
                }
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

    private void createTravelPathsForOrderItems(Connection c, int orderId, int buyerCityId) throws SQLException {
        Graph g = new Graph();
        createVertices(c, g);
        createEdges(c, g);

        Map<String, Line> lines = getLines(c);

        Map<Vertex, Integer> distances = g.getShortestDistances(buyerCityId);

        int minDistance = Integer.MAX_VALUE;
        int closestCityWithShop = -1;
        Iterator<Map.Entry<Vertex, Integer>> distanceIterator = distances.entrySet().iterator();
        while(distanceIterator.hasNext()){
            Map.Entry<Vertex, Integer> entry = distanceIterator.next();
            if(shopCities.contains(entry.getKey().id) && entry.getValue() < minDistance) closestCityWithShop = entry.getKey().id;
        }
        if(closestCityWithShop == -1) throw new RuntimeException("shopCities is empty.");

        Pair<LinkedList<Vertex>, Integer> buyerToShopPath = g.findShortestPath(buyerCityId, closestCityWithShop);

        Calendar assemblyDate = getCurrentDate();
        int maxNumOfDays = Integer.MIN_VALUE;

        Iterator<Integer> cityIterator = shopCities.iterator();
        while(cityIterator.hasNext()){
            Calendar currentDate = today;
            Integer currentCity = cityIterator.next();
            if(closestCityWithShop == currentCity) continue;
            Pair<LinkedList<Vertex>, Integer> path = g.findShortestPath(closestCityWithShop, currentCity);
            ArrayList<Vertex> vertices = new ArrayList<>(path.first);
            int numOfDays = 0;
            for(int i = 0; i < vertices.size() - 1; i++){
                Line line = lines.get(Line.getHashKey(vertices.get(i).id, vertices.get(i+1).id));
                numOfDays += line.distance;
            }
            if (numOfDays > maxNumOfDays) maxNumOfDays = numOfDays;
        }
        
        assemblyDate.add(Calendar.DATE, maxNumOfDays);
    }

    private void createVertices(Connection c, Graph g) throws SQLException{
        PreparedStatement ps = c.prepareStatement("select * from [CITY]");
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            g.createVertex(rs.getInt("ID"));
        }
    }

    private void createEdges(Connection c, Graph g) throws SQLException{
        PreparedStatement ps = c.prepareStatement("select * from [LINE]");
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            g.createEdge(rs.getInt("CITY_ID1"), rs.getInt("CITY_ID2"), rs.getInt("DISTANCE"));
        }
    }

    private Map<String, Line> getLines(Connection c) throws SQLException{
        Map<String, Line> lines = new HashMap<>();
        PreparedStatement ps = c.prepareStatement("select * from [LINE]");
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            Line line = new Line(rs.getInt("ID"),
                    rs.getInt("CITY_ID1"),
                    rs.getInt("CITY_ID2"),
                    rs.getInt("DISTANCE")
            );
            lines.put(line.getHashKey(), line);
        }
        return lines;
    }

    private void insertTraveling(Connection c, Line line, Calendar currentDate){
    }

    private Calendar getCurrentDate(){
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from [SYSTEM]");
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTime(rs.getDate("CURRENT_DATE"));
                return calendar;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
