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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class encapsulates the SQL interface to the database.
 *
 * @author Andrew
 */
public abstract class SQL {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.SQL");
    private static Connection con = null;  // the connection to the database, this is shared among all instances of SQL (and its subclasses)

    /**
     * Default constructor
     */
    public SQL() {
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
     * @param command the SQL command updating the database
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
     * @param command the SQL command updating the database
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
                assert rs.next() : "No primary key returned after adding a row (SQL.addSingleRow)";
                key = rs.getInt(primaryKey);
                assert !rs.next() : "More than one primary key returned after adding a single row (SQL.addSingleRow)";
            }
        }
        return key;
    }

    /**
     * Add a single row to a table, without returning an integer primary key.
     *
     * @param command the SQL command updating the database
     * @throws java.sql.SQLException
     */
    public static final void addSingleRow(String command) throws SQLException {

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
     * @return a Result Set containing the rows
     * @throws SQLException if something happened that can't be handled
     */
    protected static ResultSet query(String table, String what, String where) throws SQLException {
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
        rs = query(sb.toString());
        return rs;
    }

    /**
     * Execute a query statement.
     *
     * @param command the SQL command querying the database
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
     * This method converts SQLExceptions to AppFatal exceptions.It returns null
     * if the error should be ignored. The code is based on that in the Oracle
     * JDBC tutorial.
     *
     * If the Log level is FINE (i.e. debug mode is set), dump the stack trace
     * to standard error.
     *
     * @param mesg a contextual message to be included in the AppFatal exception
     * @param ex the SQL exception
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
}
