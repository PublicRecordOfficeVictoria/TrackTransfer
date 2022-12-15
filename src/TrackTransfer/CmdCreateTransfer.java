package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Andrew
 */
public class CmdCreateTransfer extends SubCommand {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdCreateTransfer");
    private String desc;        // description of this delivery

    public CmdCreateTransfer() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        int key;

        database = "jdbc:h2:./trackTransfer";
        
        LOG.setLevel(null);
        config(args);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'New transfer' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -desc <description>: text describing this transfer");
            LOG.info("");
            LOG.info(" Optional:");
            LOG.info("  -db <databaseURL>: URL identifying the database (default 'jdbc:h2:./trackTransfer')");
            LOG.info("  -v: verbose mode: give more details about processing");
            LOG.info("  -d: debug mode: give a lot of details about processing");
            LOG.info("  -help: print this listing");
            LOG.info("");
            return;
        }

        // check necessary fields have been specified
        if (desc == null) {
            throw new AppFatal("Transfer description must be specified (-desc)");
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Create a new transfer");
        LOG.log(Level.INFO, " Database: {0}", database);
        LOG.log(Level.INFO, " Description: {0}", desc);
        if (LOG.getLevel() == Level.INFO) {
            LOG.info(" Logging: verbose");
        } else if (LOG.getLevel() == Level.FINE) {
            LOG.info(" Logging: debug");
        } else {
            LOG.info(" Logging: warnings & errors only");
        }

        // connect to the database and create the tables
        connectDB(database);
        try {
            TblTransfer.createTable();
        } catch (SQLException sqe) {
            if (sqe.getSQLState().equals("42S01") && sqe.getErrorCode() == 42101) {
                throw new AppError("Failed CreateTransfer: Transfer has already been created ('"+database+"')");
            }
        }
        TblDelivery.createTable();
        TblItem.createTable();
        TblEvent.createTable();
        TblItemEvent.createTable();
        
        key = TblTransfer.add(desc);
        
        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " Database created ({0})", database);
        LOG.log(Level.INFO, " Transfer (key={0})", key);
    }

    public void config(String args[]) throws AppFatal {
        String usage = "-db <databaseURL> -desc <text>";
        int i, j;

        // process remaining command line arguments
        i = 1;
        try {
            while (i < args.length) {
                switch (args[i].toLowerCase()) {

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

                    // get output directory
                    case "-db":
                        i++;
                        database = args[i];
                        i++;
                        break;

                    case "-desc":
                        i++;
                        desc = args[i];
                        i++;
                        break;

                    // otherwise check to see if it is a common argument
                    default:
                        throw new AppFatal("Unrecognised argument '" + args[i] + "'. Usage: " + usage);
                }
            }
        } catch (ArrayIndexOutOfBoundsException ae) {
            throw new AppFatal("Missing argument. Usage: " + usage);
        }
    }
}
