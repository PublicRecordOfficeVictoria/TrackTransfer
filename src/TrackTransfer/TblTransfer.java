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
 * This class encapsulates the Transfer table in the SQL database. It allows the
 * table to be created, rows added, queried, and dropped.
 *
 * @author Andrew
 */
public class TblTransfer extends SQL {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblTransfer");

    static String CREATE_TRANSFER_TABLE
            = "create table TRANSFER ("
            + "TRANSFER_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "DESC varchar(100) NOT NULL" // arbitrary description
            + ")";

    /**
     * Initialise the Transfer Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblTransfer() throws SQLException {
        super();
    }
    
    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     * 
     * @throws SQLException 
     */

    public static void createTable() throws SQLException {
        update(CREATE_TRANSFER_TABLE);
    }

    /**
     * Add a row to the transfer table
     *
     * @param desc arbitrary description
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int add(String desc) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("insert into TRANSFER (DESC) values (");
        sb.append("'");
        sb.append(desc);
        sb.append("');");
        return addSingleRow(sb.toString(), "TRANSFER_ID");
    }

    /**
     * Query the transfer table returning a result set. The result set must be
     * closed by the caller when processing the set is complete to release
     * resources.
     *
     * @param what what columns to be returned in the result set
     * @param where the conditional clause
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    public static ResultSet query(String what, String where) throws SQLException {
        return query("TRANSFER", what, where);
    }

    /**
     * Get the primary key for a row in a result set.
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
     * Dump the contents of the table
     * @return
     * @throws SQLException 
     */
    public static String printTable() throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSet rs;
        
        sb.append("TransferKey\tDescription\n");
        rs = query("*", null);
        while (rs.next()) {
            sb.append(getTransferId(rs));
            sb.append(" ");
            sb.append(getDescription(rs));
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
        update("drop TABLE if exists TRANSFER");
    }
}
