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
 * This class encapsulates the Transfer table in the SQL database. It allows the
 * table to be created, rows added, queried, and dropped.
 *
 * @author Andrew
 */
public class TblDelivery extends SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblDelivery");

    static String CREATE_DELIVERY_TABLE
            = "create table DELIVERY ("
            + "DELIVERY_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "TRANSFER_ID integer NOT NULL," // transfer delivery belongs to
            + "DESC varchar(100) NOT NULL," // arbitrary description
            + "WHEN_RECEIVED timestamp(0) with time zone NOT NULL," // date/time delivery received
            + "constraint TRANSFER_FK foreign key (TRANSFER_ID) references TRANSFER(TRANSFER_ID)"
            + ")";

    /**
     * Initialise the Delivery Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblDelivery() throws SQLException {
        super();
    }
        
    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     * 
     * @throws SQLException 
     */
    public static void createTable() throws SQLException {
        update(CREATE_DELIVERY_TABLE);
    }

    /**
     * Add a row to the delivery table
     *
     * @param transferId
     * @param desc arbitrary description
     * @return primary key of the added row
     * @throws SQLException if something happened that can't be handled
     */
    public static int add(int transferId, String desc) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("insert into DELIVERY (TRANSFER_ID, DESC, WHEN_RECEIVED) values (");
        sb.append(transferId);
        sb.append(", '");
        sb.append(desc);
        sb.append("', '");
        sb.append(sqlDateTime(0));
        sb.append("');");
        return addSingleRow(sb.toString(), "DELIVERY_ID");
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
        return query("DELIVERY", what, where);
    }

    /**
     * Get the primary key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getDeliveryId(ResultSet rs) throws SQLException {
        return rs.getInt("DELIVERY_ID");
    }

    /**
     * Get the transfer id for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getTransferId(ResultSet rs) throws SQLException {
        return rs.getInt("TRANSFER_ID");
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
        return rs.getString("WHEN_RECEIVED");
    }
    
    /**
     * Dump the contents of the table
     * @return
     * @throws SQLException 
     */
    public static String printTable() throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSet rs;
        
        sb.append("TransferKey DeliveryKey Description WhenReceived\n");
        rs = query("*", null);
        while (rs.next()) {
            sb.append(getTransferId(rs));
            sb.append(" ");
            sb.append(getDeliveryId(rs));
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
        update("drop table if exists DELIVERY");
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
