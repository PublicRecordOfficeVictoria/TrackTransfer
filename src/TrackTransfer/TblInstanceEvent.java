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
 * This class implements a many/many mapping of instances (in a delivery) and
 * events.
 *
 * @author Andrew
 */
public class TblInstanceEvent extends SQLTable {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblInstanceEvent");

    static String CREATE_INSTANCE_EVENT_TABLE
            = "create table INSTANCE_EVENT ("
            + "INSTANCE_ID integer NOT NULL, " // key of item linked to event
            + "EVENT_ID integer NOT NULL " // key of event linked to item
            + ")";

    /**
     * Initialise the Item/Event Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblInstanceEvent() throws SQLException {
        super();
    }
        
    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     * 
     * @throws SQLException 
     */
    public static void createTable() throws SQLException {
        update(CREATE_INSTANCE_EVENT_TABLE);
    }

    /**
     * Add an instance/event link to the item/event table
     *
     * @param instanceKey the instance to link
     * @param eventKey the event to link
     * @throws SQLException if something happened that can't be handled
     */
    public static void add(int instanceKey, int eventKey) throws SQLException {
        StringBuilder sb = new StringBuilder();
        
        assert instanceKey > 0;
        assert eventKey > 0;

        sb.append("insert into INSTANCE_EVENT (INSTANCE_ID, EVENT_ID) values (");
        sb.append(instanceKey);
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
     * @param orderBy how to order the results
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    public static ResultSet query(String what, String where, String orderBy) throws SQLException {
        assert what != null;
        return query("INSTANCE_EVENT", what, where, orderBy);
    }

    /**
     * Get the instance key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getInstanceId(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getInt("INSTANCE_ID");
    }

    /**
     * Get the event key for a row in a result set.
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
     * Dump the contents of the table
     * @return
     * @throws SQLException 
     */
    public static String printTable() throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSet rs;
        
        sb.append("ItemKey EventKey\n");
        rs = query("*", null, "INSTANCE_ID");
        while (rs.next()) {
            sb.append(getInstanceId(rs));
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
        update("drop table if exists INSTANCE_EVENT");
    }
}
