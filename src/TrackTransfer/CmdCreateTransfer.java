package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Create a new Transfer. This must be the first command as it creates the
 * empty database. You can't create more than one Transfer in the database.
 * 
 * @author Andrew Waugh
 */
public class CmdCreateTransfer extends Command {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdCreateTransfer");
    private String desc;        // description of this delivery
    private String usage = "-db <databaseURL> -desc <text> [-v] [-d] [-help]";

    public CmdCreateTransfer() throws AppFatal {
        super();
    }
    
    /**
     * Create a new transfer. API version. This method is called once to create
     * the databases and start a transfer. The description describes the
     * transfer.
     * 
     * Do not call this method directly, use the wrapper in the TrackTransfer
     * class.
     * 
     * @param database the string representing the database to create (cannot be null)
     * @param description a description of this transfer (e.g. an ID)
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     * @throws SQLException SQL problem occurred
     */
    public void createTransfer(String database, String description) throws AppFatal, AppError, SQLException {
        assert database != null;
        assert description != null;
        
        this.database = database;
        this.desc = description;
        doIt();
    }
    
    /**
     * Create a new transfer. Command line version.
     * 
     * @param args
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     * @throws SQLException SQL problem occurred
     */
    public void createTransfer(String args[]) throws AppFatal, AppError, SQLException {
        int key;

        LOG.setLevel(null);
        config(args, usage);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'New transfer' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -db <database>: Database name (default based on .mv.db file in current working directory)");
            LOG.info("  -desc <description>: text describing this transfer");
            LOG.info("");
            LOG.info(" Optional:");
            genericHelp();
            LOG.info("");
            return;
        }

        // check necessary fields have been specified
        if (database == null) {
            throw new AppError("The database must be specified (-db)");
        }
        if (desc == null) {
            throw new AppError("Transfer description must be specified (-desc)");
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Create a new transfer");
        LOG.log(Level.INFO, " Database: {0}", database);
        LOG.log(Level.INFO, " Description: {0}", desc);
        genericStatus();
        
        key = doIt();

        // acknowledge creation
        LOG.log(Level.INFO, " Database created ({0})", database);
        LOG.log(Level.INFO, " Transfer (key={0})", key);
    }

    /**
     * Internal function that actually does the work.
     * 
     * @throws AppFatal
     * @throws AppError
     * @throws SQLException 
     */
    private int doIt() throws AppFatal, AppError, SQLException {
        int key;
        
        // connect to the database and create the tables
        connectDB();
        try {
            TblTransfer.createTable();
        } catch (SQLException sqe) {
            if (sqe.getSQLState().equals("42S01") && sqe.getErrorCode() == 42101) {
                throw new AppError("Failed CreateTransfer: Transfer has already been created ('" + database + "')");
            }
            throw sqe;
        }
        TblDelivery.createTable();
        TblInstance.createTable();
        TblEvent.createTable();
        TblInstanceEvent.createTable();
        TblItem.createTable();
        TblKeyword.createTable();
        TblItemKeyword.createTable();

        key = TblTransfer.add(desc);
        disconnectDB();
        
        return key;
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
        int j;

        switch (args[i].toLowerCase()) {
            // description of the items to be annotated
            case "-desc":
                i++;
                desc = args[i];
                i++;
                j = 2;
                break;
            // otherwise complain
            default:
                j = 0;
        }
        return j;
    }
}
