/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * This class encapsulates the Transfer table in the SQL database. It allows the
 * table to be created, rows added, queried, and dropped.
 *
 * @author Andrew
 */
public class TblDelivery extends SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblDelivery");
    
    static final int MAX_FILEPATH_LEN = 2560;
    static final int MAX_DESC_LEN = 100;
    
    static String CREATE_DELIVERY_TABLE
            = "create table DELIVERY ("
            + "DELIVERY_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "TRANSFER_ID integer NOT NULL," // transfer delivery belongs to
            + "FILEPATH varchar("+MAX_FILEPATH_LEN+") NOT NULL," // pathname of the root of the delivery tree
            + "DESC varchar("+MAX_DESC_LEN+") NOT NULL," // arbitrary description
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
    public static int add(int transferId, String desc, Path root) throws SQLException {
        StringBuilder sb = new StringBuilder();
        String s;
        
        assert transferId > 0;
        assert desc != null;
        assert root != null;
        
        desc = truncate("Description", desc, MAX_DESC_LEN);
        s = truncate("Filepath", root.toString(), MAX_FILEPATH_LEN);

        sb.append("insert into DELIVERY (TRANSFER_ID, FILEPATH, DESC, WHEN_RECEIVED) values (");
        sb.append(transferId);
        sb.append(", '");
        sb.append(encode(s));
        sb.append("', '");
        sb.append(encode(desc));
        sb.append("', '");
        sb.append(getSQLTimeStamp(0));
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
     * @param orderBy how to order the results
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    public static ResultSet query(String what, String where, String orderBy) throws SQLException {
        assert what != null;
        
        return query("DELIVERY", what, where, orderBy);
    }

    /**
     * Get the primary key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getDeliveryId(ResultSet rs) throws SQLException {
        assert rs != null;
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
        assert rs != null;
        return rs.getInt("TRANSFER_ID");
    }
    
    /**
     * Get the root (filepath) of the delivery
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static Path getRootPath(ResultSet rs) throws SQLException {
        assert rs != null;
        return Paths.get(unencode(rs.getString("FILEPATH")));
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
     * Get when the delivery was received.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getWhenReceived(ResultSet rs) throws SQLException {
        assert rs != null;
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
        
        sb.append("TransferKey DeliveryKey WhenReceived RootDirectory Desc\n");
        rs = query("*", null, "TRANSFER_ID");
        while (rs.next()) {
            sb.append(getTransferId(rs));
            sb.append(" ");
            sb.append(getDeliveryId(rs));
            sb.append(" ");
            sb.append(getWhenReceived(rs));
            sb.append(" ");
            sb.append(getRootPath(rs).toString());
            sb.append(" ");
            sb.append(getDescription(rs));
            sb.append(" ");
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

}
