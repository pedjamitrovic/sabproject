package student;

import operations.GeneralOperations;

import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class mp150608_GeneralOperations implements GeneralOperations {
    @Override
    public void setInitialTime(Calendar time) {
        PreparedStatement ps = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("insert into [SYSTEM] values(?)");
            ps.setDate(1, new java.sql.Date(time.getTimeInMillis()));
            if (ps.executeUpdate() == 0) {
                c.rollback();
                throw new RuntimeException("Execute update returned 0 updated rows.");
            }
            c.commit();
            c.setAutoCommit(true);
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
    }

    @Override
    public Calendar time(int days) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            c.setAutoCommit(false);
            ps = c.prepareStatement("select * from [SYSTEM]");
            rs = ps.executeQuery();
            if (rs.next()) {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTime(rs.getDate("CURRENT_DATE"));
                calendar.add(Calendar.DATE, days);

                ps = c.prepareStatement("update [SYSTEM] set [CURRENT_DATE] = ?");
                ps.setDate(1, new java.sql.Date(calendar.getTimeInMillis()));
                if (ps.executeUpdate() == 0) {
                    c.rollback();
                    throw new RuntimeException("Execute update returned 0 updated rows.");
                }
                c.commit();
                c.setAutoCommit(true);
                return calendar;
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
        return null;
    }

    @Override
    public Calendar getCurrentTime() {
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
    public void eraseAll() {
        CallableStatement cs = null;
        try (Connection c = DriverManager.getConnection(Settings.connectionUrl)){
            cs = c.prepareCall("{call SP_TRUNCATE_ALL_TABLES}");
            cs.execute();
        } catch (SQLException e) {
            if (e instanceof SQLException) e.printStackTrace();
        }
        finally{
            try {
                if (cs != null) cs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
