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
public class TblItemEvent extends SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblItemEvent");

    static String CREATE_ITEM_EVENT_TABLE
            = "create table ITEM_EVENT ("
            + "ITEM_ID integer NOT NULL, " // key of item linked to event
            + "EVENT_ID integer NOT NULL " // key of event linked to item
            + ")";

    /**
     * Initialise the Item/Event Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblItemEvent() throws SQLException {
        super();
    }
        
    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     * 
     * @throws SQLException 
     */
    public static void createTable() throws SQLException {
        update(CREATE_ITEM_EVENT_TABLE);
    }

    /**
     * Add an item/event link to the item event table
     *
     * @param itemKey
     * @param eventKey
     * @throws SQLException if something happened that can't be handled
     */
    public static void add(int itemKey, int eventKey) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("insert into ITEM_EVENT (ITEM_ID, EVENT_ID) values (");
        sb.append(itemKey);
        sb.append(", ");
        sb.append(eventKey);
        sb.append(");");
        addSingleRow(sb.toString());
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
        return query("ITEM_EVENT", what, where);
    }

    /**
     * Get the item key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getItemId(ResultSet rs) throws SQLException {
        return rs.getInt("ITEM_ID");
    }

    /**
     * Get the event key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getEventId(ResultSet rs) throws SQLException {
        return rs.getInt("EVENT_ID");
    }
    
    /**
     * Dump the contents of the table
     * @return
     * @throws SQLException 
     */
    public static String printTable() throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSet rs;
        
        sb.append("ItemKey EventKey\n");
        rs = query("*", null);
        while (rs.next()) {
            sb.append(getItemId(rs));
            sb.append(" ");
            sb.append(getEventId(rs));
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
        update("drop table if exists ITEM_EVENT");
    }
}
