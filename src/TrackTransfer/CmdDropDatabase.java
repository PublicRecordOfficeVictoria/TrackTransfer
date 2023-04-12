package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Drops the database and all the data contained in it. This is primarily
 * intended for testing.
 * 
 * @author Andrew Waugh
 */
public class CmdDropDatabase extends Command {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdDropDatabase");
    private final String usage = "-db <database> [-v] [-d] [-help]";

    public CmdDropDatabase() throws AppFatal {
        super();
    }
    
    /**
     * Drop the database deleting any data in it. API version. The database has
     * to be explicitly specified to prevent accidents.
     * 
     * Do not call this method directly, use the wrapper in the TrackTransfer
     * class.
     * 
     * @param database the string representing the database to connect to
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     * @throws SQLException SQL problem occurred
     */
    public void dropDatabase(String database) throws AppFatal, AppError, SQLException {
        assert database != null;
        this.database = database;
        doIt();
    }
    
    /**
     * Drop the database, deleting any data in it. Command line version.
     * 
     * @param args
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     * @throws SQLException SQL problem occurred
     */
    public void dropDatabase (String args[]) throws AppFatal, AppError, SQLException {
        config(args, usage);

        // just asked for help?
        if (help) {
            LOG.info("'New transfer' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -db <database>: Database name (default based on .mv.db file in current working directory)");
            LOG.info("");
            LOG.info(" Optional:");
            genericHelp();
            LOG.info("");
            return;
        }

        // check necessary fields have been specified
        if (database == null) {
            throw new AppError("URL for database has not been specified (-db)");
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Drop database");
        LOG.log(Level.INFO, " Database: {0}", database);
        genericStatus();
        
        doIt();

        // acknowledge creation
        LOG.log(Level.INFO, " Database dropped ({0})", database);
    }
    
    /**
     * Internal function that actually does the work.
     * 
     * @throws AppFatal
     * @throws AppError
     * @throws SQLException 
     */
    private void doIt() throws AppFatal, AppError, SQLException {
        
        // connect to the database and drop the tables
        connectDB();
        TblItemKeyword.dropTable();
        TblKeyword.dropTable();
        TblItem.dropTable();
        TblInstanceEvent.dropTable();
        TblEvent.dropTable();
        TblInstance.dropTable();
        TblDelivery.dropTable();
        TblTransfer.dropTable();
        disconnectDB();
    }
    
    /**
     * Process command line arguments specific to this command. Passed the array
     * of command line arguments, and the current position in the array. Returns
     * the number of arguments consumed (0 = nothing matched)
     * 
     * @param args command line arguments
     * @param i position in command line arguments
     * @return command line arguments consumed
     * @throws AppError
     * @throws ArrayIndexOutOfBoundsException 
     */
    @Override
    int specificConfig(String[] args, int i) throws AppError, ArrayIndexOutOfBoundsException {
        return 0;
    }
}
