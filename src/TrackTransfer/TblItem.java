/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class encapsulates the Item table in the SQL database. A row in the Item
 * table represents an item that has been received during the transfer. Some
 * (hopefully most) of these items are records, the rest are junk. Data
 * associated with an item include its identity, and its ultimate fate. Items
 * are also represented by one or more linked Instances. An Instance is an
 * example of the item received in a Delivery. The same item may be received
 * multiple times over many deliveries - for example an instance of a item may
 * be resubmitted because the earlier version was incorrect or broken. Or it may
 * be resubmitted as a duplicate in error.
 *
 * @author Andrew
 */
public class TblItem extends SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblItem");

    static final int MAX_FILENAME_LEN = 200;
    static final int MAX_HASHALG_LEN = 10;
    static final int MAX_HASH_LEN = 100;
    static final int MAX_STATUS_LEN = 20;

    static String CREATE_ITEM_TABLE
            = "create table ITEM ("
            + "ITEM_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "FILENAME varchar(" + MAX_FILENAME_LEN + ") NOT NULL, " // (file) name of the item
            + "HASHALG varbinary(" + MAX_HASHALG_LEN + "), " // algorithm used to calculate hash value
            + "HASH varchar(" + MAX_HASH_LEN + "), " // hash value of the record
            + "IS_RECORD boolean, " // true if this item is a record
            + "IS_FINALISED boolean, " // true if this item has been finalised
            + "STATUS varchar(" + MAX_STATUS_LEN + "), " // status of this item
            + "CURRENT_INSTANCE_ID integer NOT NULL, " // last instance of this item that we saw
            + "ACTIVE_INSTANCE_ID integer NOT NULL, " // the current active instance of this item
            + "constraint CURRENT_INSTANCE_FK foreign key (CURRENT_INSTANCE_ID) references INSTANCE(INSTANCE_ID), "
            + "constraint ACTIVE_INSTANCE_FK foreign key (ACTIVE_INSTANCE_ID) references INSTANCE(INSTANCE_ID)"
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
     * Add a row to the item table. The arguments are the identifier of the
     * record, its initial status, and the link to first instance of the record
     * found in a delivery. At least one of filename or hash must be not null.
     *
     * @param filename of the record (null if not used)
     * @param hashAlg hash algorithm used to generate the hash (null if hash not
     * present)
     * @param hash hash value (null if not used)
     * @param isRecord true if this item is a record
     * @param isFinalised true if this item has been finalised
     * @param status initial status of the record (cannot be null)
     * @param instanceId key of the first instance of this record
     * @return primary key of the added row
     * @throws SQLException if something happened that can't be handled
     */
    public static int add(String filename, String hashAlg, String hash, boolean isRecord, boolean isFinalised, String status, int instanceId) throws SQLException {
        StringBuilder sb = new StringBuilder();

        // check invariants
        assert filename != null || (hash != null && hashAlg != null);
        assert status != null;
        assert instanceId > 1;

        filename = truncate("Filename", filename, MAX_FILENAME_LEN);
        hashAlg = truncate("Hash algorithm", hashAlg, MAX_HASHALG_LEN);
        hash = truncate("Hash", hash, MAX_HASH_LEN);
        status = truncate("Status", status, MAX_STATUS_LEN);

        sb.append("insert into ITEM (FILENAME, HASHALG, HASH, IS_RECORD, IS_FINALISED, STATUS, CURRENT_INSTANCE_ID, ACTIVE_INSTANCE_ID) values ('");
        sb.append(encode(filename));
        sb.append("', '");
        sb.append(encode(hashAlg));
        sb.append("', '");
        sb.append(hash);
        sb.append("', ");
        sb.append(isRecord ? "TRUE" : "FALSE");
        sb.append(", ");
        sb.append(isFinalised ? "TRUE" : "FALSE");
        sb.append(", '");
        sb.append(encode(status));
        sb.append("', ");
        sb.append(instanceId);
        sb.append(", ");
        sb.append(instanceId);
        sb.append(");");
        return addSingleRow(sb.toString(), "ITEM_ID");
    }

    /**
     * Find an item given an identifier (filename and/or hash). At least one of
     * filename and hash must be present. If both are present, both must match.
     * We ignore a trailing ".lnk" in the file name - this idendifies a short
     * cut in Windows.
     *
     * @param filename the filename to look for (may be null)
     * @param hash the hash to look for (may be null)
     * @return a result set containing all the columns of the item
     * @throws SQLException
     */
    public static ResultSet findItem(String filename, String hash) throws SQLException {
        ResultSet rs;
        int i;

        assert filename != null || hash != null;
        
        if ((i = filename.toLowerCase().lastIndexOf(".lnk")) != -1) {
            filename = filename.substring(0, i);
        }

        if (filename != null && hash == null) {
            rs = query("*", "FILENAME = '" + encode(filename) + "'", null);
        } else if (filename == null && hash != null) {
            rs = query("*", "HASH = '" + hash + "'", null);
        } else {
            rs = query("*", "FILENAME = '" + encode(filename) + "' AND HASH = '" + hash + "'", null);
        }
        return rs;
    }

    /**
     * Get the primary key for a row in a result set.
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
     * Get the file name for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getFilename(ResultSet rs) throws SQLException {
        assert rs != null;
        return unencode(rs.getString("FILENAME"));
    }

    /**
     * Get the hash algorithm for a row in a result set (may be null).
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getHashAlg(ResultSet rs) throws SQLException {
        assert rs != null;
        return unencode(rs.getString("HASHALG"));
    }

    /**
     * Get the hash value for a row in a result set (may be null).
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getHash(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getString("HASH");
    }

    /**
     * Is this item a record
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static boolean isRecord(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getBoolean("IS_RECORD");
    }

    /**
     * Set whether this item is a record
     *
     * @param key key of item to change (must be > 0)
     * @param isRecord new value of isRecord
     * @throws SQLException
     */
    public static void setIsRecord(int key, boolean isRecord) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;

        sb.append("update ITEM set IS_RECORD='");
        sb.append(isRecord ? 'Y' : 'N');
        sb.append("' where ITEM_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Is this item finalised
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static boolean isFinalised(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getBoolean("IS_FINALISED");
    }

    /**
     * Set this item to be finalised. This means the active instance will not
     * change
     *
     * @param key key of item to change (must be > 0)
     * @param isFinalised new value of isRecord
     * @throws SQLException
     */
    public static void setIsFinalised(int key, boolean isFinalised) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;

        sb.append("update ITEM set IS_FINALISED=");
        sb.append(isFinalised ? "TRUE" : "FALSE");
        sb.append("' where ITEM_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Get the status of a row in a result set (may be null).
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getStatus(ResultSet rs) throws SQLException {
        assert rs != null;
        return unencode(rs.getString("STATUS"));
    }

    /**
     * Set the status of an item.
     *
     * @param key key of item to change (must be > 0)
     * @param status new status (must not be null)
     * @param isFinalised true if the item has been finalised
     * @throws SQLException
     */
    public static void setStatus(int key, String status, boolean isFinalised) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;
        assert status != null;
        if (status.length() > MAX_STATUS_LEN) {
            LOG.log(Level.WARNING, "Truncated status ({0}) longer than {1}", new Object[]{status, MAX_STATUS_LEN});
            status = status.substring(0, MAX_STATUS_LEN);
        }

        sb.append("update ITEM set STATUS='");
        sb.append(encode(status));
        sb.append("', IS_FINALISED=");
        sb.append(isFinalised);
        sb.append(" where ITEM_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Get the current instance of an item.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getCurrentInstanceId(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getInt("CURRENT_INSTANCE_ID");
    }

    /**
     * Set the current instance of an item.
     *
     * @param key key of item to change (must be > 0)
     * @param currentInstance new current instance (must be > 0)
     * @throws SQLException
     */
    public static void setCurrentInstance(int key, int currentInstance) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;
        assert currentInstance > 0;

        sb.append("update ITEM set CURRENT_INSTANCE_ID='");
        sb.append(currentInstance);
        sb.append("' where ITEM_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Get the active instance of an item.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getActiveInstanceId(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getInt("ACTIVE_INSTANCE_ID");
    }

    /**
     * Set the active instance of an item.
     *
     * @param key key of item to change (must be > 0)
     * @param activeInstance new current instance (must be > 0)
     * @throws SQLException
     */
    public static void setActiveInstance(int key, int activeInstance) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;
        assert activeInstance > 0;

        sb.append("update ITEM set ACTIVE_INSTANCE_ID='");
        sb.append(activeInstance);
        sb.append("' where ITEM_ID=");
        sb.append(key);
        sb.append(";");
        update(sb.toString());
    }

    /**
     * Return a string describing this item for a report
     *
     * @param rs
     * @return
     * @throws java.sql.SQLException
     */
    public static String reportItem(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("Item: '");
        sb.append(TblItem.getFilename(rs));
        sb.append("' ");
        if (TblItem.isFinalised(rs)) {
            sb.append("FINALISED ");
        } else if (!TblItem.isRecord(rs)) {
            sb.append("NOT A RECORD ");
        } else {
            sb.append("PROCESSING ");
        }
        sb.append("(Status: ");
        sb.append(TblItem.getStatus(rs));
        sb.append(")");

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

        sb.append("ItemKey Filename HashAlg Hash isRecord isFinalised Status Current Instance\n");
        rs = query("*", null, "ITEM_ID");
        while (rs.next()) {
            sb.append(getItemId(rs));
            sb.append(" ");
            sb.append(getFilename(rs));
            sb.append(" ");
            sb.append(getHashAlg(rs));
            sb.append(" ");
            sb.append(getHash(rs));
            sb.append(" ");
            sb.append(isRecord(rs));
            sb.append(" ");
            sb.append(isFinalised(rs));
            sb.append(" ");
            sb.append(getStatus(rs));
            sb.append(" ");
            sb.append(getCurrentInstanceId(rs));
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Query the Item table returning a result set. The result set must be
     * closed by the caller when processing the set is complete to release
     * resources.
     *
     * @param what what columns to be returned in the result set
     * @param where the conditional clause (may be null)
     * @param orderBy the ordering clause (may be null)
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    public static ResultSet query(String what, String where, String orderBy) throws SQLException {
        assert what != null;
        return query("ITEM", what, where, orderBy);
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
