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
 * This class encapsulates the Item table in the SQL database. It allows the
 * table to be created, rows added, queried, and dropped.
 *
 * @author Andrew
 */
public class TblItem extends SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblItem");

    static String CREATE_ITEM_TABLE
            = "create table ITEM ("
            + "ITEM_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "FILENAME varchar(200) NOT NULL," // (file) name of the item
            + "HASHALG varchar(20),"             // algorithm used to calculate hash value
            + "HASH varchar(100),"              // hash value of the item
            + "STATUS varchar(20),"              // status of this item
            + "FILEPATH varchar(2560) NOT NULL," // pathname of the item
            + "DELIVERY_ID integer NOT NULL," // transfer delivery belongs to
            + "constraint DELIVERY_FK foreign key (DELIVERY_ID) references DELIVERY(DELIVERY_ID)"
            + ");";
    static String CREATE_ITEM_FILENAME_INDEX
            = "create unique index IDX_FILENAME on ITEM (FILENAME);";
    /**
     * Initialise the Item Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblItem() throws SQLException {
        super();
    }
        
    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     * 
     * @throws SQLException 
     */
    public static void createTable() throws SQLException {
        update(CREATE_ITEM_TABLE);
        update(CREATE_ITEM_FILENAME_INDEX);
    }

    /**
     * Add a row to the item table
     *
     * @param filename
     * @param hashAlg
     * @param hash
     * @param status
     * @param filepath
     * @param deliveryId    Delivery this item was received in
     * @return primary key of the added row
     * @throws SQLException if something happened that can't be handled
     */
    public static int add(String filename, String hashAlg, String hash, String status, String filepath, int deliveryId) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("insert into ITEM (FILENAME, HASHALG, HASH, STATUS, FILEPATH, DELIVERY_ID) values ('");
        sb.append(filename);
        sb.append("', '");
        sb.append(hashAlg);
        sb.append("', '");
        sb.append(hash);
        sb.append("', '");
        sb.append(status);
        sb.append("', '");
        sb.append(filepath);
        sb.append("', ");
        sb.append(deliveryId);
        sb.append(");");
        return addSingleRow(sb.toString(), "ITEM_ID");
    }
    
    /**
     * Change the status of an item.
     * 
     * @param key key of item to change
     * @param status new status
     * @throws SQLException 
     */
    public static void changeStatus(int key, String status) throws SQLException {
        StringBuilder sb = new StringBuilder();
        
        sb.append("update ITEM set STATUS='");
        sb.append(status);
        sb.append("' where ITEM_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Query the item table returning a result set. The result set must be
     * closed by the caller when processing the set is complete to release
     * resources.
     *
     * @param what what columns to be returned in the result set
     * @param where the conditional clause
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    public static ResultSet query(String what, String where) throws SQLException {
        return query("ITEM", what, where);
    }

    /**
     * Get the primary key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getItemId(ResultSet rs) throws SQLException {
        return rs.getInt("ITEM_ID");
    }
    
    /**
     * Get the file name for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getFilename(ResultSet rs) throws SQLException {
        return rs.getString("FILENAME");
    }
    
    /**
     * Get the hash algorithm for a row in a result set (may be null).
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getHashalg(ResultSet rs) throws SQLException {
        return rs.getString("HASHALG");
    }
    
    /**
     * Get the hash value for a row in a result set (may be null).
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getHash(ResultSet rs) throws SQLException {
        return rs.getString("HASH");
    }
    
    /**
     * Get the status of a row in a result set (may be null).
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getStatus(ResultSet rs) throws SQLException {
        return rs.getString("STATUS");
    }
        
    /**
     * Get the status of a row in a result set (may be null).
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getFilepath(ResultSet rs) throws SQLException {
        return rs.getString("FILEPATH");
    }
    
    /**
     * Get the transfer id for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getDeliveryId(ResultSet rs) throws SQLException {
        return rs.getInt("DELIVERY_ID");
    }
    
    public static int itemExists(String filename) throws SQLException {
        ResultSet rs;
        
        rs = query("ITEM_ID", "FILENAME = '"+filename+"'");
        if (!rs.next()) {
            return 0;
        }
        return getItemId(rs);
    }
    
    /**
     * Dump the contents of the table
     * @return
     * @throws SQLException 
     */
    public static String printTable() throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSet rs;
        
        sb.append("ItemKey Filename HashAlg Hash Status DeliveryKey Filepath\n");
        rs = query("*", null);
        while (rs.next()) {
            sb.append(getItemId(rs));
            sb.append(" ");
            sb.append(getFilename(rs));
            sb.append(" ");
            sb.append(getHashalg(rs));
            sb.append(" ");
            sb.append(getHash(rs));
            sb.append(" ");
            sb.append(getStatus(rs));
            sb.append(" ");
            sb.append(getDeliveryId(rs));
            sb.append(" ");
            sb.append(getFilepath(rs));
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
        update("drop index if exists IDX_FILENAME");
        update("drop TABLE if exists ITEM");
    }
}
