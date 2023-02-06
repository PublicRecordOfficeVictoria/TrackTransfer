/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class encapsulates the Instance table in the SQL database. A row in the
 * Instance table is a file received in a delivery. Some instances are instances
 * of a Record. Data associated with an Instance is its pathname within the
 * Delivery and its status. The status has two parts: an internal status, and an
 * external status (set by the user). The internal status consists of four
 * flags. The first (IS_NOT_RECORD) is set if the file does not contain a
 * record. The second (IS_DUPLICATE) is set for the second and subsequent
 * duplicates of a file in the delivery (i.e one duplicate will not have this
 * flag set, the other duplicates will). The third flag (IS_SUPERSEDED) if the
 * same file has appeared in a subsequent delivery. The final flag
 * (IS_FINALISED) will be set if this file appears in a previous delivery that
 * has already been ingested.
 *
 * @author Andrew
 */
public class TblInstance extends SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblInstance");

    static final int MAX_FILEPATH_LEN = 2560;
    static final int MAX_STATUS_LEN = 20;

    static String CREATE_INSTANCE_TABLE
            = "create table INSTANCE ("
            + "INSTANCE_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "DELIVERY_ID integer NOT NULL," // transfer delivery belongs to
            + "ITEM_ID integer, "             // item instance belongs to (may be 0)
            + "FILEPATH varchar(" + MAX_FILEPATH_LEN + ") NOT NULL," // pathname of the instance (within a delivery)
            + "IS_DUPLICATE boolean,"         // true if this instance is to be ignored as it is a duplicate in this delivery
            + "IS_SUPERSEDED boolean,"        // true if this instance has been superseded by an instance in a later delivery
            + "PREVIOUS_INSTANCE integer,"    // previous instance of this record (0 if null)
            + "constraint DELIVERY_FK foreign key (DELIVERY_ID) references DELIVERY(DELIVERY_ID)"
            + ");";

    /**
     * Initialise the Instance Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblInstance() throws SQLException {
        super();
    }

    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     *
     * @throws SQLException
     */
    public static void createTable() throws SQLException {
        update(CREATE_INSTANCE_TABLE);
    }

    /**
     * Add a new instance to the table
     *
     * @param deliveryId the delivery the file is part of
     * @param itemId the item this instance belongs to
     * @param filepath the pathname of the file (relative to the root of the delivery)
     * @param duplicate true if this file is a duplicate of another in this delivery
     * @param prevInstanceId id of previous instance of this item
     * @return primary key of the added row
     * @throws SQLException if something happened that can't be handled
     */
    public static int add(int deliveryId, int itemId, String filepath, boolean duplicate, int prevInstanceId) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert deliveryId > 0;
        assert filepath != null;

        filepath = truncate("File path", filepath, MAX_FILEPATH_LEN);

        sb.append("insert into INSTANCE (DELIVERY_ID, ITEM_ID, FILEPATH, IS_DUPLICATE, IS_SUPERSEDED, PREVIOUS_INSTANCE) values (");
        sb.append(deliveryId);
        sb.append(", ");
        sb.append(itemId);
        sb.append(", '");
        sb.append(encode(filepath));
        sb.append("', '");
        sb.append(duplicate ? "Y" : "N");
        sb.append("', '");
        sb.append("N");
        sb.append("', ");
        sb.append(prevInstanceId);
        sb.append(");");
        return addSingleRow(sb.toString(), "INSTANCE_ID");
    }

    /**
     * Return the instance identified by the id.
     *
     * @param id of instance
     * @return instance matching key (may be null in none match)
     * @throws SQLException
     */
    public static ResultSet getInstance(int id) throws SQLException {
        assert id > 0;
        return query("INSTANCE", "*", "INSTANCE_ID=" + id, null);
    }

    /**
     * Get the primary key for an instance in a result set.
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
     * Get the delivery id for an instance in a result set.
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
     * Get the item id for an instance in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getItemId(ResultSet rs) throws SQLException {
        assert rs != null;

        return rs.getInt("ITEM_ID");
    }

    /**
     * Set the item id of an instance.
     *
     * @param key key of item to change
     * @param itemId item id (may be null)
     * @throws SQLException
     */
    public static void setItemId(int key, int itemId) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;
        assert itemId >= 0;

        sb.append("update INSTANCE set ITEM_ID=");
        sb.append(itemId);
        sb.append(" where INSTANCE_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Get the file path for an instance in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getFilepath(ResultSet rs) throws SQLException {
        assert rs != null;
        return unencode(rs.getString("FILEPATH"));
    }

    /**
     * Is the instance in a result set a duplicate?
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static boolean isDuplicate(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getBoolean("IS_DUPLICATE");
    }

    /**
     * This instance has been duplicated by a later instance
     *
     * @param key key of item to change
     * @throws SQLException
     */
    public static void setIsDuplicated(int key) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;

        sb.append("update INSTANCE set IS_DUPLICATE='Y' where INSTANCE_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Is the instance in a result set superseded?
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static boolean isSuperseded(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getBoolean("IS_SUPERSEDED");
    }

    /**
     * This instance has been superseded
     *
     * @param key key of item to change
     * @throws SQLException
     */
    public static void setIsSuperseded(int key) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;

        sb.append("update INSTANCE set IS_SUPERSEDED='Y' where INSTANCE_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Get an instance by giving the path
     *
     * @param p
     * @return
     * @throws SQLException
     */
    public static int getInstanceByFilepath(Path p) throws SQLException {
        ResultSet rs;
        String s;

        s = p.toString().replace("'", "''");
        rs = query("INSTANCE_ID", "FILEPATH = '" + s + "'", null);
        assert rs != null;
        return rs.getInt("INSTANCE_ID");
    }

    /**
     * Return a string describing this instance for a report
     *
     * @param rs
     * @return
     * @throws java.sql.SQLException
     */
    public static String reportInstance(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("Instance: ");
        if (TblInstance.isDuplicate(rs)) {
            sb.append("DUPLICATE ");
        } else if (TblInstance.isSuperseded(rs)) {
            sb.append("SUPERSEDED ");
        } else {
            sb.append("LATEST ");
        }
        sb.append("Original location '");
        sb.append(TblInstance.getFilepath(rs));
        sb.append("'");

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

        sb.append("InstanceKey DeliveryKey ItemId Filepath Duplicate Superseded Finalised\n");
        rs = query("*", null, "INSTANCE_ID");
        while (rs.next()) {
            sb.append(getInstanceId(rs));
            sb.append(" ");
            sb.append(getDeliveryId(rs));
            sb.append(" ");
            sb.append(getItemId(rs));
            sb.append(" ");
            sb.append(getFilepath(rs));
            sb.append(" ");
            sb.append(isDuplicate(rs));
            sb.append(" ");
            sb.append(isSuperseded(rs));
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Query the instance table returning a result set. The result set must be
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

        return query("INSTANCE", what, where, orderBy);
    }

    /**
     * Drop the table and all data.
     *
     * @throws SQLException if something happened that can't be handled
     */
    public static void dropTable() throws SQLException {
        update("drop TABLE if exists INSTANCE");
    }
}
