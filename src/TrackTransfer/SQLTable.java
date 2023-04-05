/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import VERSCommon.AppFatal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class encapsulates the SQLTable interface to the database.
 *
 * @author Andrew
 */
public abstract class SQLTable {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.SQLTable");
    private static Connection con = null;  // the connection to the database, this is shared among all instances of SQLTable (and its subclasses)
    
    // constants for common fields
    protected static final int MAX_DESC_LEN = 200;
    protected static final int MAX_FILEPATH_LEN = 2560;

    /**
     * Default constructor
     */
    public SQLTable() {
    }

    /**
     * Connect to the SQL database. This can be done on a static object before
     * creating any instances of a subclass (tables).
     *
     * @param url the URL to connect to (e.g. "jdbc:h2:./test/testDB")
     * @throws AppFatal
     */
    public static void connect(String url) throws AppFatal {
        assert con == null;
        try {
            con = DriverManager.getConnection(url);
        } catch (SQLException sqle) {
            throw createAppFatal("Failed opening connection to database: ", sqle, "SQL.connect");
        }
        LOG.log(Level.FINE, "Connected to SQL database: {0}", new Object[]{url});
    }

    /**
     * Disconnect from the SQL database. This can be done on a static object.
     * Can later reconnect if desired.
     *
     * @throws SQLException if something happened that can't be handled
     */
    public static void disconnect() throws SQLException {
        assert con != null;
        con.close();
        con = null;
    }

    /**
     * Execute an update statement.
     *
     * @param command the SQLTable command updating the database
     * @return the result set returned
     * @throws SQLException if something happened that can't be handled
     */
    public static final ResultSet update(String command) throws SQLException {
        ResultSet rs;

        assert con != null;
        try (Statement stmt = con.createStatement()) {
            stmt.closeOnCompletion();
            stmt.executeUpdate(command, Statement.RETURN_GENERATED_KEYS);
            rs = stmt.getGeneratedKeys();
            assert rs != null;
        }
        return rs;
    }

    /**
     * Add a single row to a table, returning an integer primary key.
     *
     * @param command the SQLTable command updating the database
     * @param primaryKey the column name of the integer primary key
     * @return the primary key of the added row
     * @throws java.sql.SQLException
     */
    public static final int addSingleRow(String command, String primaryKey) throws SQLException {
        int key;

        try (Statement stmt = con.createStatement()) {
            stmt.closeOnCompletion();
            stmt.executeUpdate(command, Statement.RETURN_GENERATED_KEYS);
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                rs.next();
                // assert rs.next() : "No primary key returned after adding a row (SQLTable.addSingleRow)";
                key = rs.getInt(primaryKey);
                // assert !rs.next() : "More than one primary key returned after adding a single row (SQLTable.addSingleRow)";
            }
        }
        return key;
    }

    /**
     * Add a single row to a table, without returning an integer primary key.
     *
     * @param command the SQLTable command updating the database
     * @throws java.sql.SQLException
     */
    public static final void addSingleRow(String command) throws SQLException {

        try (Statement stmt = con.createStatement()) {
            stmt.closeOnCompletion();
            stmt.executeUpdate(command);
        }
    }
    
    /**
     * Remove a single row from a table
     *
     * @param command the SQLTable command updating the database
     * @throws java.sql.SQLException
     */
    public static final void removeRows(String command) throws SQLException {

        try (Statement stmt = con.createStatement()) {
            stmt.closeOnCompletion();
            stmt.executeUpdate(command);
        }
    }

    /**
     * Query a table returning a result set.The result set must be closed by the
     * caller when processing the set is complete to release resources.
     *
     * @param table table to query
     * @param what what columns to be returned in the result set
     * @param where the conditional clause
     * @param orderBy the ordering clause (may be null)
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    protected static ResultSet query(String table, String what, String where, String orderBy) throws SQLException {
        StringBuilder sb = new StringBuilder();
        ResultSet rs;

        sb.append("select ");
        sb.append(what);
        if (table != null) {
            sb.append(" from ");
            sb.append(table);
        }
        if (where != null) {
            sb.append(" where ");
            sb.append(where);
        }
        if (orderBy != null) {
            sb.append(" order by ");
            sb.append(orderBy);
        }
        rs = query(sb.toString());
        return rs;
    }

    /**
     * Execute a query statement.
     *
     * @param command the SQLTable command querying the database
     * @return the ResultSet containing the results of the query
     * @throws SQLException if something happened that can't be handled
     */
    public static ResultSet query(String command) throws SQLException {
        ResultSet rs;
        Statement stmt;

        stmt = con.createStatement();
        stmt.closeOnCompletion();
        rs = stmt.executeQuery(command);
        assert rs != null;
        return rs;
    }
    
    /**
     * Get an SQL TIMESTAMP. If ms is 0, get the current date/time, otherwise
     * convert the ms since the Java epoch. Note that SQL wants a ':' between
     * the hours and minutes of a time zone, whereas Java doesn't have anything
     *
     * @param ms the timestamp in milliseconds
     * @return
     */
    protected static String getSQLTimeStamp(long ms) {
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
    
    /**
     * Encode single quotes to be double quotes in a string to be put into an
 SQLTable database
     * @param s
     * @return 
     */
    protected static String encode(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("'", "''");
    }
    
    /**
     * Unencode double quotes to be single quotes in strings from an SQLTable database
     * @param s
     * @return 
     */
    protected static String unencode(String s) {
        return s.replace("'", "'");
    }
    
    /**
     * Truncate a string value if longer than the field length
     * 
     * @param desc
     * @param s
     * @param maxlen
     * @return 
     */
    protected static String truncate(String desc, String s, int maxlen) {
        if (s != null && s.length()>maxlen) {
            LOG.log(Level.WARNING, "{0} ({0}) truncated as longer than {1}", new Object[]{desc, s, maxlen});
            s = s.substring(0, maxlen);
        }
        return s;
    }

    /**
     * This method converts SQLExceptions to AppFatal exceptions.It returns null
     * if the error should be ignored. The code is based on that in the Oracle
     * JDBC tutorial.
     *
     * If the Log level is FINE (i.e. debug mode is set), dump the stack trace
     * to standard error.
     *
     * @param mesg a contextual message to be included in the AppFatal exception
     * @param ex the SQLTable exception
     * @param method the method generating the exception
     * @return the created AppFatal
     */
    public static AppFatal createAppFatal(String mesg, SQLException ex, String method) {
        String sqlState;
        StringBuilder sb = new StringBuilder();

        if (mesg != null) {
            sb.append(mesg);
            sb.append("\n");
        }
        for (Throwable t : ex) {
            if (t instanceof SQLException) {
                SQLException e = (SQLException) t;
                sqlState = e.getSQLState();
                if (sqlState != null) {
                    // X0Y32: Jar file already exists in schema
                    if (sqlState.equalsIgnoreCase("X0Y32")) {
                        return null;
                    }
                    // 42Y55: Table already exists in schema
                    if (sqlState.equalsIgnoreCase("42Y55")) {
                        return null;
                    }
                }
                if (LOG.getLevel() == Level.FINER) {
                    t.printStackTrace(System.err);
                }
                sb.append("SQLState: ");
                sb.append(sqlState);
                sb.append(" Error code: ");
                sb.append(e.getErrorCode());
                sb.append("\nMessage: ");
                sb.append(e.getMessage());
                sb.append("\n");
            }
        }
        sb.append(" (");
        sb.append(method);
        sb.append(")");
        return new AppFatal(sb.toString());
    }
    
    public static void dumpRSMetadata(ResultSet rs) throws SQLException {
        ResultSetMetaData rsm = rs.getMetaData();
        int i;
        
        System.out.println("Column count: "+rsm.getColumnCount());
        for (i=1; i<rsm.getColumnCount()+1; i++) {
            System.out.print(rsm.getColumnName(i)+" ");
            System.out.println(rsm.getColumnTypeName(i));
        }
    }
}
