package student;

import operations.CityOperations;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class mp150608_CityOperations implements CityOperations {

    @Override
    public int createCity(String name) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from CITY where NAME = ?");
            ps.setString(1, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return -1;

            String[] generatedColumns = { "ID" };
            ps = c.prepareStatement("insert into CITY values(?)", generatedColumns);
            ps.setString(1, name);
            if (ps.executeUpdate() == 0) return -1;
            try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                if (generatedKeys.next()) return generatedKeys.getInt("ID");
                else return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Integer> getCities() {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select ID from CITY");
            ResultSet rs = ps.executeQuery();
            List<Integer> cities = new LinkedList<>();
            while(rs.next()){
                cities.add(rs.getInt("ID"));
            }
            return cities;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int connectCities(int cityId1, int cityId2, int distance) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from LINE where (CITY_ID1 = ? AND CITY_ID2 = ?) OR (CITY_ID1 = ? AND CITY_ID2 = ?)");
            ps.setInt(1, cityId1);
            ps.setInt(2, cityId2);
            ps.setInt(3, cityId2);
            ps.setInt(4, cityId1);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return -1;

            String[] generatedColumns = { "ID" };
            ps = c.prepareStatement("insert into LINE values(?, ?, ?)", generatedColumns);
            ps.setInt(1, cityId1);
            ps.setInt(2, cityId2);
            ps.setInt(3, distance);
            if (ps.executeUpdate() == 0) return -1;
            try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                if (generatedKeys.next()) return generatedKeys.getInt("ID");
                else return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public List<Integer> getConnectedCities(int cityId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from LINE where CITY_ID1 = ? OR CITY_ID2 = ?");
            ps.setInt(1, cityId);
            ps.setInt(2, cityId);
            ResultSet rs = ps.executeQuery();
            List<Integer> cities = new LinkedList<>();
            while(rs.next()){
                if(rs.getInt("CITY_ID1") == cityId) cities.add(rs.getInt("CITY_ID2"));
                else cities.add(rs.getInt("CITY_ID1"));
            }
            return cities;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Integer> getShops(int cityId) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select ID from SHOP where CITY_ID = ?");
            ps.setInt(1, cityId);
            ResultSet rs = ps.executeQuery();
            List<Integer> cities = new LinkedList<>();
            while(rs.next()){
                if(rs.getInt("CITY_ID1") == cityId) cities.add(rs.getInt("CITY_ID2"));
                else cities.add(rs.getInt("CITY_ID1"));
            }
            return cities;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
