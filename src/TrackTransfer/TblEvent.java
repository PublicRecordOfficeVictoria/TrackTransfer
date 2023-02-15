/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * This class encapsulates the Event table in the SQL database. It allows the
 * table to be created, rows added, queried, and dropped.
 *
 * @author Andrew
 */
public class TblEvent extends SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblEvent");

    static final int MAX_DESC_LEN = 100;

    static String CREATE_EVENT_TABLE
            = "create table EVENT ("
            + "EVENT_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "DESC varchar(" + MAX_DESC_LEN + ") NOT NULL," // description of event
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

        assert desc != null;

        desc = truncate("Description", desc, MAX_DESC_LEN);

        sb.append("insert into EVENT (DESC, OCCURRED) values ('");
        sb.append(encode(desc));
        sb.append("', '");
        sb.append(getSQLTimeStamp(0));
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
     * @param orderBy how to order the results
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    public static ResultSet query(String what, String where, String orderBy) throws SQLException {
        assert what != null;
        return query("EVENT", what, where, orderBy);
    }

    /**
     * Get the primary key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getEventId(ResultSet rs) throws SQLException {
        assert rs != null;
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
        assert rs != null;
        return unencode(rs.getString("DESC"));
    }

    /**
     * Get the description for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getWhenReceived(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getString("OCCURRED");
    }

    /**
     * Return a string describing this event for a report
     *
     * @param rs
     * @return
     * @throws java.sql.SQLException
     */
    public static String reportEvent(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("Event: ");
        sb.append(TblEvent.getWhenReceived(rs));
        sb.append(" ");
        sb.append(TblEvent.getDescription(rs));

        return sb.toString();
    }

    /**
     * Dump the contents of the table
     *
     * @return
     * @throws SQLException
     */
    public static String printTable() throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSet rs;

        sb.append("EventKey Description WhenOccurrred\n");
        rs = query("*", null, "EVENT_ID");
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
}
