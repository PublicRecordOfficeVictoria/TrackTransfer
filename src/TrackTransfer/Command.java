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
 * This class has the common routines for a sub command within Track Transfer
 *
 * @author Andrew
 */
public abstract class Command {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.Command");
    protected boolean help;       // true if help has been requested
    protected boolean veo;        // true if only considering V2 or V3 VEOs as records
    protected String database;    // database connected to
    static final String DB_PREFIX = "jdbc:h2:";

    protected Command() {
        help = false;
    }

    /**
     * Connect to the database.The required database name is of the form
     * 'jdbc:h2:F/fileName' which results in opening a database file
     * 'F/fileName.mv.db'.(Note the 'h2' relates to the implementation of JDBC;
     *  if this is replaced, that string must be replaced.)
     * 
     *  Users can 'specify' the database to connect to in two ways. The first is
     *  to specify the 'name' of the database (without the prefixing 'jdbc:h2:'.
     *  If a name is not specified (passed null), the current working directory
     *  is searched for a file ending in '.mv.db'. This is then used as the
     *  directory name. If there are multiple '.mv.db' files in the current
     *  directory, we complain.
     *
     * @return the name of the database connected to
     * @returns the database name connected to
     * @throws SQLException problem connecting to the database
     * @throws AppError multiple databases found
     * @throws AppFatal shouldn't happen
     */
    protected String connectDB() throws SQLException, AppError, AppFatal {
        Path cwd;
        String fn, fp, dbName;
        int i;

        if (database == null) {
            cwd = Paths.get(System.getProperty("user.dir"));
            assert cwd != null;

            // go through the items in the directory
            dbName = null;
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
        } else if (!database.contains(":")) {
            dbName = Paths.get(database).toAbsolutePath().toString().replaceAll("\\\\", "/");
            database = DB_PREFIX + dbName;
        }
        LOG.log(Level.FINE, " Connecting to database: {0}", database);

        SQLTable.connect(database);
        return database;
    }

    /**
     * Disconnect from the database
     *
     * @throws SQLException
     */
    protected void disconnectDB() throws SQLException {
        SQLTable.disconnect();
    }

    /**
     * Do common configuration for all commands.
     *
     * @param args
     * @param usage
     * @throws AppError
     */
    protected void config(String[] args, String usage) throws AppError {
        int i, j;

        // process remaining command line arguments
        veo = false;
        help = false;
        database = null;
        i = 1;
        try {
            while (i < args.length) {
                switch (args[i].toLowerCase()) {
                    // if we want to only process VEOs...
                    case "-veo":
                        veo = true;
                        i++;
                        break;
                    case "-db":
                        i++;
                        database = args[i];
                        i++;
                        System.out.println("Dabatase: '"+database+"'");
                        break;
                    // if verbose mode...
                    case "-v":
                        LOG.setLevel(Level.INFO);
                        i++;
                        break;
                    // if debugging...
                    case "-d":
                        LOG.setLevel(Level.FINE);
                        i++;
                        break;
                    // write a summary of the command line options to the std out
                    case "-help":
                        help = true;
                        i++;
                        break;
                    // otherwise check for command specific options
                    default:
                        if ((j = specificConfig(args, i)) == 0) {
                            throw new AppError("Unrecognised argument '" + args[i] + "'. Usage: " + usage);
                        }
                        i += j;
                }
            }
        } catch (ArrayIndexOutOfBoundsException ae) {
            throw new AppError("Missing argument. Usage: " + usage);
        }
    }

    /**
     * Process command line arguments specific to a command. Passed the array of
     * command line arguments, and the current position in the array. Returns
     * the number of arguments consumed (0 = nothing matched)
     *
     * @param args command line arguments
     * @param i position in command line arguments
     * @return command line arguments consumed
     * @throws AppError
     * @throws ArrayIndexOutOfBoundsException
     */
    abstract int specificConfig(String[] args, int i) throws AppError, ArrayIndexOutOfBoundsException;

    /**
     * Give help about the generic command line options
     */
    protected void genericHelp() {
        LOG.info("  -v: verbose mode: give more details about processing");
        LOG.info("  -d: debug mode: give a lot of details about processing");
        LOG.info("  -help: print this listing");
        LOG.info("");
    }

    protected void genericStatus() {
        if (LOG.getLevel() == Level.INFO) {
            LOG.info(" Logging: verbose");
        } else if (LOG.getLevel() == Level.FINE) {
            LOG.info(" Logging: debug");
        } else {
            LOG.info(" Logging: warnings & errors only");
        }
    }
}
