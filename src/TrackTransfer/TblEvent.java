/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Logger;

/**
 * This class encapsulates the Event table in the SQL database. It allows the
 * table to be created, rows added, queried, and dropped.
 *
 * @author Andrew
 */
public class TblEvent extends SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblEvent");

    static String CREATE_EVENT_TABLE
            = "create table EVENT ("
            + "EVENT_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "DESC varchar(100) NOT NULL,"         // description of event
            + "OCCURRED timestamp(0) with time zone NOT NULL" // date/time of event
            + ")";

    /**
     * Initialise the Delivery Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblEvent() throws SQLException {
        super();
    }
        
    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     * 
     * @throws SQLException 
     */
    public static void createTable() throws SQLException {
        update(CREATE_EVENT_TABLE);
    }

    /**
     * Add an event to the event table
     *
     * @param desc arbitrary description
     * @return primary key of the added row
     * @throws SQLException if something happened that can't be handled
     */
    public static int add(String desc) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("insert into EVENT (DESC, OCCURRED) values ('");
        sb.append(desc);
        sb.append("', '");
        sb.append(sqlDateTime(0));
        sb.append("');");
        return addSingleRow(sb.toString(), "EVENT_ID");
    }

    /**
     * Query the delivery table returning a result set. The result set must be
     * closed by the caller when processing the set is complete to release
     * resources.
     *
     * @param what what columns to be returned in the result set
     * @param where the conditional clause
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    public static ResultSet query(String what, String where) throws SQLException {
        return query("EVENT", what, where);
    }

    /**
     * Get the primary key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getEventId(ResultSet rs) throws SQLException {
        return rs.getInt("EVENT_ID");
    }

    /**
     * Get the description for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getDescription(ResultSet rs) throws SQLException {
        return rs.getString("DESC");
    }

    /**
     * Get the description for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getWhenReceived(ResultSet rs) throws SQLException {
        return rs.getString("OCCURRED");
    }
    
    /**
     * Dump the contents of the table
     * @return
     * @throws SQLException 
     */
    public static String printTable() throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSet rs;
        
        sb.append("EventKey Description WhenOccurrred\n");
        rs = query("*", null);
        while (rs.next()) {
            sb.append(getEventId(rs));
            sb.append(" ");
            sb.append(getDescription(rs));
            sb.append(" ");
            sb.append(getWhenReceived(rs));
            sb.append("\n");
        }
        return sb.toString();
    } 

    /**
     * Drop the table and all data.
     *
     * @throws SQLException if something happened that can't be handled
     */
    public static void dropTable() throws SQLException {
        update("drop table if exists EVENT");
    }

    /**
     * Get an SQL TIMESTAMP
     *
     * @param ms
     * @return
     */
    private static String sqlDateTime(long ms) {
        Date d;
        SimpleDateFormat sdf;
        TimeZone tz;
        String s1;

        tz = TimeZone.getDefault();
        sdf = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ssZ");
        sdf.setTimeZone(tz);
        if (ms == 0) {
            d = new Date();
        } else {
            d = new Date(ms);
        }
        s1 = sdf.format(d);
        return s1.substring(0, 22) + ":" + s1.substring(22, 24);
    }
}
