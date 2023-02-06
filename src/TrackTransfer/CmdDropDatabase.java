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
public class CmdDropDatabase extends SubCommand {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdDropDatabase");
    private String database;    // database being connected to (may be null)

    public CmdDropDatabase() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        
        config(args);

        // just asked for help?
        if (help) {
            System.out.println("'New transfer' command line arguments:");
            System.out.println(" Mandatory:");
            System.out.println("  -db <databaseURL>: URL identifying the database (e.g. 'jdbc:h2:./test/testDB'");
            System.out.println("");
            System.out.println(" Optional:");
            System.out.println("  -v: verbose mode: give more details about processing");
            System.out.println("  -d: debug mode: give a lot of details about processing");
            System.out.println("  -help: print this listing");
            System.out.println("");
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
        if (LOG.getLevel() == Level.INFO) {
            LOG.info(" Logging: verbose");
        } else if (LOG.getLevel() == Level.FINE) {
            LOG.info(" Logging: debug");
        } else {
            LOG.info(" Logging: warnings & errors only");
        }

        // connect to the database and drop the tables
        database = connectDB(database);
        TblItem.dropTable();
        TblInstanceEvent.dropTable();
        TblEvent.dropTable();
        TblInstance.dropTable();
        TblDelivery.dropTable();
        TblTransfer.dropTable();
        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " Database dropped ({0})", database);
    }

    public void config(String args[]) throws AppError {
        String usage = "-db <databaseURL> [-v] [-d] [-help]";
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

                    // otherwise check to see if it is a common argument
                    default:
                        throw new AppError("Unrecognised argument '" + args[i] + "'. Usage: " + usage);
                }
            }
        } catch (ArrayIndexOutOfBoundsException ae) {
            throw new AppError("Missing argument. Usage: " + usage);
        }
    }
}
