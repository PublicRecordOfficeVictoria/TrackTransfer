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
 * This class encapsulates the Keyword table in the SQL database. It allows the
 * table to be created, rows added, queried, and dropped.
 *
 * @author Andrew
 */
public class TblKeyword extends SQLTable {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.TblKeyword");
    
    private final static int MAX_KEYWORD_LEN = 20; // max length of a keyword in chars

    private final static String CREATE_KEYWORD_TABLE
            = "create table KEYWORD ("
            + "KEYWORD_ID integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " // primary key
            + "KEYWORD varchar(" + MAX_KEYWORD_LEN + ") NOT NULL UNIQUE" // keyword
            + ")";
    private final static String CREATE_KEYWORD_INDEX
            = "create unique index IDX_KEYWORD on KEYWORD (KEYWORD);";

    /**
     * Initialise the Delivery Table
     *
     * @throws SQLException if something happened that can't be handled
     */
    public TblKeyword() throws SQLException {
        super();
    }

    /**
     * Create the table. Only needs to be done once when the database is being
     * created.
     *
     * @throws SQLException
     */
    public static void createTable() throws SQLException {
        update(CREATE_KEYWORD_TABLE);
        update(CREATE_KEYWORD_INDEX);
    }

    /**
     * Add a keyword to the keyword table
     *
     * @param keyword name of keyword
     * @return primary key of the added row
     * @throws SQLException if something happened that can't be handled
     */
    public static int add(String keyword) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert keyword != null;

        keyword = truncate("Keyword", keyword, MAX_KEYWORD_LEN);

        sb.append("insert into KEYWORD (KEYWORD) values ('");
        sb.append(encode(keyword));
        sb.append("');");
        return addSingleRow(sb.toString(), "KEYWORD_ID");
    }
    
    /**
     * Remove a keyword from the keyword table
     * 
     * @param key keyword to remove
     * @throws SQLException 
     */
    public static void remove(int key) throws SQLException {
        StringBuilder sb = new StringBuilder();

        assert key > 0;
        sb.append("delete from KEYWORD where KEYWORD_ID=");
        sb.append(key);
        sb.append(";");
        removeRows(sb.toString());
    }

    /**
     * Query the keyword table returning a result set. The result set must be
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
        return query("KEYWORD", what, where, orderBy);
    }

    /**
     * Get the primary key for a row in a result set.
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
     * Get the keyword for a row in a result set.
     *
     * @param rs
     * @return
     * @throws SQLException if something happened that can't be handled
     */
    public static String getKeyword(ResultSet rs) throws SQLException {
        assert rs != null;
        return unencode(rs.getString("KEYWORD"));
    }

    /**
     * Return a string describing this keyword for a report
     *
     * @param rs
     * @return
     * @throws java.sql.SQLException
     */
    public static String reportKeyword(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append("Keyword: '");
        sb.append(TblKeyword.getKeyword(rs));
        sb.append("'");

        return sb.toString();
    }
    
    /**
     * Return a string describing this item for a report
     *
     * @param rs
     * @return
     * @throws java.sql.SQLException
     */
    public static String[] tableOut(ResultSet rs) throws SQLException {
        String[] s = new String[1];

        if (rs == null) {
            s[0] = "Keyword";
        } else {
            s[0] = TblKeyword.getKeyword(rs);
        }
        return s;
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

        sb.append("KeywordId Keyword\n");
        rs = query("*", null, "KEYWORD_ID");
        while (rs.next()) {
            sb.append(getKeywordId(rs));
            sb.append(" ");
            sb.append(getKeyword(rs));
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
        update("drop table if exists KEYWORD");
    }
}
