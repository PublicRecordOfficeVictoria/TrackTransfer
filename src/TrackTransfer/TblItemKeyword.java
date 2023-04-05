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
 * This class implements a many/many mapping of items and keywords
 *
 * @author Andrew
 */
public class TblItemKeyword extends SQLTable {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblItemKeyword");

    static String CREATE_ITEM_KEYWORD_TABLE
            = "create table ITEM_KEYWORD ("
            + "ITEM_ID integer NOT NULL, " // key of item linked to event
            + "KEYWORD_ID integer NOT NULL, " // key of event linked to item
            + "CONSTRAINT ITEM_KEYWORD_U UNIQUE (ITEM_ID, KEYWORD_ID)" // ensure that a keyword can only be mapped once
            + ")";

    /**
     * Initialise the Item/Keyword Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblItemKeyword() throws SQLException {
        super();
    }

    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     *
     * @throws SQLException
     */
    public static void createTable() throws SQLException {
        update(CREATE_ITEM_KEYWORD_TABLE);
    }

    /**
     * Add an item/keyword link to the item/keyword table. It is quite likely
     * that the user will attempt to add a keyword multiple times; this is
     * trapped here and ignored.
     *
     * @param itemKey the item to link
     * @param keywordKey the keyword to link
     * @throws SQLException if something happened that can't be handled
     */
    public static void add(int itemKey, int keywordKey) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert itemKey > 0;
        assert keywordKey > 0;

        sb.append("insert into ITEM_KEYWORD (ITEM_ID, KEYWORD_ID) values (");
        sb.append(itemKey);
        sb.append(", ");
        sb.append(keywordKey);
        sb.append(");");
        try {
            addSingleRow(sb.toString());
        } catch (SQLException sqe) {
            // 23505 is adding a duplicate value; the user has attempted to add
            // a duplicate keyword. We ignore this.
            if (!sqe.getSQLState().equals("23505")) {
                throw sqe;
            }
        }
    }

    /**
     * Remove a link between an item and a keyword
     *
     * @param itemKey item
     * @param keywordKey keyword
     * @throws SQLException
     */
    public static void remove(int itemKey, int keywordKey) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert itemKey > 0;
        sb.append("delete from ITEM_KEYWORD where ITEM_ID=");
        sb.append(itemKey);
        sb.append(" AND KEYWORD_ID=");
        sb.append(keywordKey);
        sb.append(";");
        removeRows(sb.toString());
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
        return query("ITEM_KEYWORD", what, where, orderBy);
    }

    /**
     * Get the item key for a row in a result set.
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
     * Get the keyword key for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static int getKeywordId(ResultSet rs) throws SQLException {
        assert rs != null;
        return rs.getInt("KEYWORD_ID");
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

        sb.append("ItemKey KeywordKey\n");
        rs = query("*", null, "ITEM_ID");
        while (rs.next()) {
            sb.append(getItemId(rs));
            sb.append(" ");
            sb.append(getKeywordId(rs));
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
        update("drop table if exists ITEM_KEYWORD");
    }
}
