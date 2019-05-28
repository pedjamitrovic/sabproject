package student;

import operations.GeneralOperations;

import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class mp150608_GeneralOperations implements GeneralOperations {
    @Override
    public void setInitialTime(Calendar time) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("insert into SYSTEM values(?)");
            ps.setDate(1, new java.sql.Date(time.getTimeInMillis()));
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Calendar time(int days) {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from SYSTEM");
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTime(rs.getDate("CURRENT_DATE"));
                calendar.add(Calendar.DATE, days);
                
                ps = c.prepareStatement("update SYSTEM set CURRENT_DATE = ?");
                ps.setDate(1, new java.sql.Date(calendar.getTimeInMillis()));
                ps.executeUpdate();
                return calendar;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Calendar getCurrentTime() {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("select * from SYSTEM");
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

    @Override
    public void eraseAll() {
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            PreparedStatement ps = c.prepareStatement("delete from SYSTEM");
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
