package student;

import operations.CityOperations;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class mp150608_CityOperations implements CityOperations {

    @Override
    public int createCity(String name) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("select * from CITY where NAME = ?");
            ps.setString(1, name);
            rs = ps.executeQuery();
            if (rs.next()) {
                c.rollback();
                throw new RuntimeException("City with provided name already exists.");
            }

            ps = c.prepareStatement("insert into CITY values(?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
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
            if(e instanceof SQLException) e.printStackTrace();
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
    public List<Integer> getCities() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select ID from CITY");
            rs = ps.executeQuery();
            List<Integer> cities = new LinkedList<>();
            while(rs.next()){
                cities.add(rs.getInt("ID"));
            }
            return cities;
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
    public int connectCities(int cityId1, int cityId2, int distance) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("select * from LINE where (CITY_ID1 = ? AND CITY_ID2 = ?) OR (CITY_ID1 = ? AND CITY_ID2 = ?)");
            ps.setInt(1, cityId1);
            ps.setInt(2, cityId2);
            ps.setInt(3, cityId2);
            ps.setInt(4, cityId1);
            rs = ps.executeQuery();
            if (rs.next()) {
                c.rollback();
                throw new RuntimeException("Line already exists.");
            }

            ps = c.prepareStatement("insert into LINE values(?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setInt(1, cityId1);
            ps.setInt(2, cityId2);
            ps.setInt(3, distance);
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
    public List<Integer> getConnectedCities(int cityId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from LINE where CITY_ID1 = ? OR CITY_ID2 = ?");
            ps.setInt(1, cityId);
            ps.setInt(2, cityId);
            rs = ps.executeQuery();
            List<Integer> cities = new LinkedList<>();
            while(rs.next()){
                if(rs.getInt("CITY_ID1") == cityId) cities.add(rs.getInt("CITY_ID2"));
                else cities.add(rs.getInt("CITY_ID1"));
            }
            return cities;
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
    public List<Integer> getShops(int cityId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            ps = c.prepareStatement("select * from SHOP where CITY_ID = ?");
            ps.setInt(1, cityId);
            rs = ps.executeQuery();
            List<Integer> shops = new LinkedList<>();
            while(rs.next()){
                shops.add(rs.getInt("ID"));
            }
            return shops;
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
}
