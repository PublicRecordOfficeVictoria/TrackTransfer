package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Andrew
 */
public class CmdCreateTransfer extends SubCommand {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdCreateTransfer");
    private String database;    // database being connected to (may be null)
    private String desc;        // description of this delivery
    private String usage = "-db <databaseURL> -desc <text> [-v] [-d] [-help]";

    public CmdCreateTransfer() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
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

        // connect to the database and create the tables
        database = connectDB(database);
        try {
            TblTransfer.createTable();
        } catch (SQLException sqe) {
            if (sqe.getSQLState().equals("42S01") && sqe.getErrorCode() == 42101) {
                throw new AppError("Failed CreateTransfer: Transfer has already been created ('" + database + "')");
            }
        }
        TblDelivery.createTable();
        TblInstance.createTable();
        TblEvent.createTable();
        TblInstanceEvent.createTable();
        TblItem.createTable();

        key = TblTransfer.add(desc);
        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " Database created ({0})", database);
        LOG.log(Level.INFO, " Transfer (key={0})", key);
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
            // get output directory
            case "-db":
                i++;
                database = args[i];
                i++;
                j = 2;
                break;
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
