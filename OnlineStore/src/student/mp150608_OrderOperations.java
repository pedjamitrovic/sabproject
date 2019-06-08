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
        PreparedStatement psCheckOrderValidity = null, ps = null;
        ResultSet rsCheckOrderValidity = null, rsArticle = null, rsOrderItem = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            psCheckOrderValidity = c.prepareStatement("select * from [ORDER] as O where O.ID = ? and O.STATE = ?");
            psCheckOrderValidity.setInt(1, orderId);
            psCheckOrderValidity.setString(2, "created");
            rsCheckOrderValidity = psCheckOrderValidity.executeQuery();
            if (!rsCheckOrderValidity.next()) {
                c.rollback();
                throw new RuntimeException("Order is not valid.");
            }
            int articleQuantity = -1;
            int discount = -1;
            ps = c.prepareStatement("select * from ARTICLE join SHOP on ARTICLE.SHOP_ID = SHOP.ID where ARTICLE.ID = ?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, articleId);
            rsArticle = ps.executeQuery();
            if (!rsArticle.next()) {
                c.rollback();
                throw new RuntimeException("Article is not valid.");
            }
            else{
                articleQuantity = rsArticle.getInt("QUANTITY");
                if(articleQuantity < count) {
                    c.rollback();
                    throw new RuntimeException("Insufficient article quantity.");
                }
                discount = rsArticle.getInt("DISCOUNT");
            }
            ps = c.prepareStatement("select * from ORDER_ITEM where ORDER_ID = ? and ARTICLE_ID = ?", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setInt(1, orderId);
            ps.setInt(2, articleId);
            rsOrderItem = ps.executeQuery();
            if(!rsOrderItem.next()){
                ps = c.prepareStatement("insert into ORDER_ITEM values(?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
                ps.setInt(1, orderId);
                ps.setInt(2, articleId);
                ps.setInt(3, count);
                ps.setInt(4, discount);
                if (ps.executeUpdate() == 0) {
                    c.rollback();
                    throw new RuntimeException("Execute update returned 0 updated rows.");
                }
                rsArticle.updateInt("QUANTITY", articleQuantity - count);
                rsArticle.updateRow();
                try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        c.commit();
                        return generatedKeys.getInt(1);
                    }
                    else {
                        c.rollback();
                        throw new RuntimeException("Generated keys error.");
                    }
                }
            }
            else{
                rsOrderItem.updateInt("QUANTITY", articleQuantity + count);
                rsOrderItem.updateRow();
                rsArticle.updateInt("QUANTITY", articleQuantity - count);
                rsArticle.updateRow();
                c.commit();
                c.setAutoCommit(true);
                return rsOrderItem.getInt("ID");
            }
        } catch (SQLException | RuntimeException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (psCheckOrderValidity != null) psCheckOrderValidity.close();
                if (ps != null) ps.close();
                if (rsCheckOrderValidity != null) rsCheckOrderValidity.close();
                if (rsArticle != null) rsArticle.close();
                if (rsOrderItem != null) rsOrderItem.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public int removeArticle(int orderId, int articleId) {
        PreparedStatement ps = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("delete from ORDER_ITEM where ORDER_ID = ? AND ARTICLE_ID = ?");
            ps.setInt(1, orderId);
            ps.setInt(2, articleId);
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
        finally {
            try {
                if (ps != null) ps.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public List<Integer> getItems(int orderId) {
        PreparedStatement ps = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from ORDER_ITEM where ORDER_ID = ?");
            ps.setInt(1, orderId);
            ResultSet rs = ps.executeQuery();
            List<Integer> items = new LinkedList<>();
            while(rs.next()){
                items.add(rs.getInt("ID"));
            }
            return items;
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (ps != null) ps.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int completeOrder(int orderId) {
        PreparedStatement ps = null, psCheckBuyerCredit = null;
        CallableStatement cs = null;
        ResultSet rsCheckBuyerCredit = null, rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            cs = c.prepareCall("{call SP_FINAL_PRICE(?, ?)}");
            cs.setInt(1, orderId);
            cs.registerOutParameter(2, Types.DECIMAL);
            cs.execute();
            BigDecimal orderPrice = cs.getBigDecimal(2);
            psCheckBuyerCredit = c.prepareStatement("select * from BUYER as B join [ORDER] as O on B.ID = O.BUYER_ID where O.ID = ?");
            psCheckBuyerCredit.setInt(1, orderId);
            rsCheckBuyerCredit = psCheckBuyerCredit.executeQuery();

            int buyerId;
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
            rs = ps.executeQuery();
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
            c.setAutoCommit(true);
            return 1;
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (ps != null) ps.close();
                if (psCheckBuyerCredit != null) psCheckBuyerCredit.close();
                if (cs != null) cs.close();
                if (rsCheckBuyerCredit != null) rsCheckBuyerCredit.close();
                if (rs != null) rs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public BigDecimal getFinalPrice(int orderId) {
        CallableStatement cs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            cs = c.prepareCall("{call SP_FINAL_PRICE(?, ?)}");
            cs.setInt(1, orderId);
            cs.registerOutParameter(2, Types.DECIMAL);
            cs.execute();
            return cs.getBigDecimal(2).setScale(3);
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (cs != null) cs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public BigDecimal getDiscountSum(int orderId) {
        CallableStatement cs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            cs = c.prepareCall("{call SP_DISCOUNT_SUM(?, ?)}");
            cs.setInt(1, orderId);
            cs.registerOutParameter(2, Types.DECIMAL);
            cs.execute();
            return cs.getBigDecimal(2).setScale(3);
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (cs != null) cs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public String getState(int orderId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [ORDER] where ID = ?");
            ps.setInt(1, orderId);
            rs = ps.executeQuery();
            if (!rs.next()) return null;
            else return rs.getString("STATE");
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public Calendar getSentTime(int orderId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [ORDER] where ID = ?");
            ps.setInt(1, orderId);
            rs = ps.executeQuery();
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
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public Calendar getRecievedTime(int orderId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [ORDER] where ID = ? and RECEIVED_time is not null");
            ps.setInt(1, orderId);
            rs = ps.executeQuery();
            if (!rs.next()) return null;
            else {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTime(rs.getDate("RECEIVED_TIME"));
                return calendar;
            }
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int getBuyer(int orderId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [ORDER] where ID = ?");
            ps.setInt(1, orderId);
            rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("BUYER");

        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public int getLocation(int orderId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [ORDER] where ID = ?");
            ps.setInt(1, orderId);
            rs = ps.executeQuery();
            if (!rs.next()) return -1;

            if(rs.getString("STATE").equals("created")) return -1;
            if(rs.getString("STATE").equals("arrived")){
                return getBuyerCityId(c, orderId);
            }
            Calendar today = getCurrentDate();
            ps = c.prepareStatement("select top(1) * from [TRAVELING] as T join [LINE] as L on T.LINE_ID = L.ID where T.ORDER_ID = ? and T.START_DATE <= ? order by [START_DATE] desc");
            ps.setInt(1, orderId);
            ps.setDate(2, new Date(today.getTimeInMillis()));
            rs = ps.executeQuery();
            if (rs.next()){
                if (rs.getInt("DIRECTION") == 0) return rs.getInt("CITY_ID1");
                else return rs.getInt("CITY_ID2");
            }
            else{
                ps = c.prepareStatement("select top(1) * from [TRAVELING] as T join [LINE] as L on T.LINE_ID = L.ID where T.ORDER_ID = ? order by [START_DATE] asc");
                ps.setInt(1, orderId);
                rs = ps.executeQuery();
                if(rs.next()){
                    if (rs.getInt("DIRECTION") == 0) return rs.getInt("CITY_ID1");
                    else return rs.getInt("CITY_ID2");
                }
                else{
                    return getBuyerCityId(c, orderId);
                }
            }
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally {
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return -1;
    }

    private void createTravelPathsForOrderItems(Connection c, int orderId, int buyerCityId) throws SQLException {
        Graph g = new Graph();
        createVertices(c, g);
        createEdges(c, g);

        Map<String, Line> lines = getLines(c);

        Map<Vertex, Integer> distances = g.getShortestDistances(buyerCityId);

        Set<Integer> shopCities = getShopsForOrder(c, orderId);

        int minDistance = Integer.MAX_VALUE;
        int closestCityWithShop = -1;
        Iterator<Map.Entry<Vertex, Integer>> distanceIterator = distances.entrySet().iterator();
        while(distanceIterator.hasNext()){
            Map.Entry<Vertex, Integer> entry = distanceIterator.next();
            if(shopCities.contains(entry.getKey().id) && entry.getValue() < minDistance) {
                closestCityWithShop = entry.getKey().id;
                minDistance = entry.getValue();
            }
        }
        if(closestCityWithShop == -1) throw new RuntimeException("shopCities is empty.");

        Pair<LinkedList<Vertex>, Integer> buyerToShopPath = g.findShortestPath(buyerCityId, closestCityWithShop);

        Calendar assemblyDate = getCurrentDate();
        int maxNumOfDays = Integer.MIN_VALUE;

        Iterator<Integer> cityIterator = shopCities.iterator();
        while(cityIterator.hasNext()) {
            Calendar currentDate = assemblyDate;
            Integer currentCity = cityIterator.next();
            if (closestCityWithShop == currentCity) continue;
            Pair<LinkedList<Vertex>, Integer> path = g.findShortestPath(closestCityWithShop, currentCity);
            ArrayList<Vertex> vertices = new ArrayList<>(path.first);
            int numOfDays = 0;
            for (int i = 0; i < vertices.size() - 1; i++) {
                Line line = lines.get(Line.getHashKey(vertices.get(i).id, vertices.get(i + 1).id));
                numOfDays += line.distance;
            }
            if (numOfDays > maxNumOfDays) maxNumOfDays = numOfDays;
        }

        assemblyDate.add(Calendar.DATE, maxNumOfDays);
        ArrayList<Vertex> vertices = new ArrayList<>(buyerToShopPath.first);
        for (int i = 0; i < vertices.size() - 1; i++) {
            int from = vertices.get(i).id;
            int to = vertices.get(i + 1).id;
            Line line = lines.get(Line.getHashKey(from, to));
            int direction;
            if (line.firstCity == from) direction = 0;
            else direction = 1;
            insertTraveling(c, assemblyDate, line, direction, orderId);
            assemblyDate.add(Calendar.DATE, line.distance);
        }
    }

    private void createVertices(Connection c, Graph g) throws SQLException{
        try (PreparedStatement ps = c.prepareStatement("select * from [CITY]");
             ResultSet rs = ps.executeQuery())
        {
            while(rs.next()) g.createVertex(rs.getInt("ID"));
        }

    }

    private void createEdges(Connection c, Graph g) throws SQLException{
        try (PreparedStatement ps = c.prepareStatement("select * from [LINE]");
             ResultSet rs = ps.executeQuery())
        {
            while (rs.next()) g.createEdge(rs.getInt("CITY_ID1"), rs.getInt("CITY_ID2"), rs.getInt("DISTANCE"));
        }
    }

    private Map<String, Line> getLines(Connection c) throws SQLException{
        try (PreparedStatement ps = c.prepareStatement("select * from [LINE]");
             ResultSet rs = ps.executeQuery())
        {
            Map<String, Line> lines = new HashMap<>();
            while (rs.next()) {
                Line line = new Line(rs.getInt("ID"),
                        rs.getInt("CITY_ID1"),
                        rs.getInt("CITY_ID2"),
                        rs.getInt("DISTANCE")
                );
                lines.put(line.getHashKey(), line);
            }
            return lines;
        }
    }

    private Set<Integer> getShopsForOrder(Connection c, int orderId) throws SQLException{
        try (PreparedStatement ps = c.prepareStatement("select distinct(S.CITY_ID) from [SHOP] as S join\n" +
                "                (select A.SHOP_ID from [ORDER_ITEM] as OI join [ARTICLE] as A\n" +
                "                        on OI.ARTICLE_ID = A.ID) as AOI\n" +
                "        on AOI.SHOP_ID = S.ID");
             ResultSet rs = ps.executeQuery())
        {
            HashSet<Integer> shops = new HashSet<>();
            while(rs.next()){
                shops.add(rs.getInt("CITY_ID"));
            }
            return shops;
        }
    }

    private void insertTraveling(Connection c, Calendar currentDate, Line line, int direction, int orderId) throws SQLException, RuntimeException{
        try (PreparedStatement ps = c.prepareStatement("insert into [TRAVELING] values(?, ?, ?, ?)"))
        {
            ps.setDate(1, new Date(currentDate.getTimeInMillis()));
            ps.setInt(2, line.id);
            ps.setInt(3, orderId);
            ps.setInt(4, direction);
            if (ps.executeUpdate() == 0) throw new RuntimeException("Execute update returned 0 updated rows.");
        }
    }

    private Calendar getCurrentDate(){
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from [SYSTEM]");
            rs = ps.executeQuery();
            if (rs.next()) {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTime(rs.getDate("CURRENT_DATE"));
                return calendar;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        return null;
    }

    private int getBuyerCityId(Connection c, int buyerId) throws SQLException{
        try (PreparedStatement ps = c.prepareStatement("select C.ID from [CITY] as C join [BUYER] as B on C.ID = B.CITY_ID where B.ID = ?"))
        {
            ps.setInt(1, buyerId);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return -1;
            else return rs.getInt("ID");
        }
    }
}
