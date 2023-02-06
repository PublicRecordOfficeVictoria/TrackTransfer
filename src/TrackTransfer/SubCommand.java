/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class has the common routines for a sub command within Track TblTransfer
 *
 * @author Andrew
 */
public class SubCommand {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.SubCommand");
    boolean help;       // true if help has been requested
    static final String DB_PREFIX = "jdbc:h2:";

    protected SubCommand() {
        help = false;
    }

    /**
     * Connect to the database.The required database name is of the form
     * 'jdbc:h2:F/fileName' which results in opening a database file
     * 'F/fileName.mv.db'. (Note the 'h2' relates to the implementation of JDBC;
     * if this is replaced, that string must be replaced.)
     *
     * Users can 'specify' the database to connect to in two ways. The first is
     * to specify the 'name' of the database (without the prefixing 'jdbc:h2:'.
     * If a name is not specified (passed null), the current working directory
     * is searched for a file ending in '.mv.db'. This is then used as the
     * directory name. If there are multiple '.mv.db' files in the current
     * directory, we complain.
     *
     * @param dbName name of database to connect to
     * @returns the database name connected to
     * @throws SQLException problem connecting to the database
     * @throws AppError multiple databases found
     * @throws AppFatal shouldn't happen
     */
    protected String connectDB(String dbName) throws SQLException, AppError, AppFatal {
        Path cwd;
        String fn, fp, database;
        int i;

        if (dbName == null) {
            cwd = Paths.get(System.getProperty("user.dir"));
            assert cwd != null;

            // go through the items in the directory
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(cwd)) {
                for (Path entry : ds) {
                    fn = entry.getFileName().toString().toLowerCase();
                    if (entry.toFile().isFile()) {
                        if (fn.endsWith(".mv.db")) {
                            if (dbName != null) {
                                throw new AppError("Database name was not specified in command line, and multiple '.mv.db' files were found in current working directory");
                            }
                            fp = entry.toAbsolutePath().toString();
                            i = fp.lastIndexOf(".mv.db");
                            assert i > 0;
                            dbName = fp.substring(0, i).replaceAll("\\\\", "/");
                        }
                    }
                }
            } catch (DirectoryIteratorException | IOException e) {
                throw new AppFatal(e.getMessage());
            }
            if (dbName == null) {
                throw new AppError("Database name was not specified in command line, but no '.mv.db' files were found in current working directory");
            }
            database = DB_PREFIX + dbName;
        } else if (dbName.contains(":")) {
            database = dbName;
        } else {
            dbName = Paths.get(dbName).toAbsolutePath().toString().replaceAll("\\\\", "/");
            database = DB_PREFIX + dbName;
        }
        LOG.log(Level.FINE, " Connecting to database: {0}", database);

        SQL.connect(database);
        return database;
    }

    /**
     * Disconnect from the database
     *
     * @throws SQLException
     */
    protected void disconnectDB() throws SQLException {
        SQL.disconnect();
    }
}
