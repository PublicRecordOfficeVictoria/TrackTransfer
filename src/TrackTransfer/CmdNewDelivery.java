package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Andrew
 */
public class CmdNewDelivery extends SubCommand {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdNewDelivery");
    private String desc;        // description of this delivery
    private Path rootDir;       // root directory containing the objects being delivered
    private int noItems;        // number of items found
    private int newItems;       // number of NEW items found

    public CmdNewDelivery() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        int transferKey, deliveryKey, eventKey;
        ResultSet rs;
 
        database = "jdbc:h2:./trackTransfer";
        noItems = 0;
        newItems = 0;
        
        config(args);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'New transfer' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -desc <description>: text describing this transfer");
            LOG.info("  -dir <filename>: name of directory holding objects being delivered");
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
            throw new AppFatal("Delivery description must be specified (-desc)");
        }
        if (rootDir == null) {
            throw new AppFatal("Directory containing the items being delivered must be specified (-dir)");
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Register a new delivery");
        LOG.log(Level.INFO, " Database: {0}", database);
        LOG.log(Level.INFO, " Description: {0}", desc);
        LOG.log(Level.INFO, " Directory of items: {0}", rootDir.toString());
        if (LOG.getLevel() == Level.INFO) {
            LOG.info(" Logging: verbose");
        } else if (LOG.getLevel() == Level.FINE) {
            LOG.info(" Logging: debug");
        } else {
            LOG.info(" Logging: warnings & errors only");
        }

        // check if the root directory is a directory and exists
        if (!rootDir.toFile().exists()) {
            throw new AppError("New Delivery: root directory '" + rootDir.toString() + "' does not exist");
        }
        if (!rootDir.toFile().isDirectory()) {
            throw new AppError("New Delivery: root directory '" + rootDir.toString() + "' is not a directory");
        }

        // connect to the database and create the tables
        connectDB(database);

        // get the key for the one transfer
        transferKey = 0;
        rs = TblTransfer.query("TRANSFER_ID", null);
        while (rs.next()) {
            transferKey = TblTransfer.getTransferId(rs);
        }

        // add the details about the delivery
        deliveryKey = TblDelivery.add(transferKey, desc);
        
        // add the delivery event
        eventKey = TblEvent.add(desc);

        // process items in the root directory
        registerItems(deliveryKey, rootDir, eventKey);

        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " Delivery added to ''{0}''; found: {1} (new: {2})", new Object[]{database, noItems, newItems});
        LOG.log(Level.INFO, " Delivery row (key={0})", deliveryKey);
    }

    public void config(String args[]) throws AppFatal {
        String usage = "-db <databaseURL> -desc <text> -dir <directory>";
        int i;

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
                    // description of the delivery
                    case "-desc":
                        i++;
                        desc = args[i];
                        i++;
                        break;
                    // directory that contains the items in the delivery
                    case "-dir":
                        i++;
                        rootDir = Paths.get(args[i]);
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

    /**
     * Register the items in this directory. If the item is, itself, a directory
     * registerItems() is called recursively.
     * 
     * @param deliveryKey the delivery this item belongs to
     * @param dir the directory being processes
     * @param eventKey the event describing this delivery
     * @throws AppFatal something went fatally wrong
     * @throws SQLException a database problem (should never occur)
     */
    private void registerItems(int deliveryKey, Path dir, int eventKey) throws AppFatal, SQLException {
        int itemKey, i;
        String s1, s2;
        Path p;
        
        // go through the items in the directory
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path entry : ds) {
                
                // recurse if the item is a directory
                if (entry.toFile().isDirectory()) {
                    registerItems(deliveryKey, entry, eventKey);
                    
                // otherwise register the item
                } else {
                    noItems++;
                    
                    // eliminate quote characters as this causes problems with SQL
                    s1 = entry.getFileName().toString().replace("'", "''");
                    p = entry.toRealPath();
                    i = p.getNameCount();
                    if (i < 2) {
                        s2 = p.toString().replace("'", "''");
                    } else {
                        s2 = p.subpath(0, i-1).toString().replace("'", "''");
                    }
                    
                    // if item doesn't already exist, add it to item table
                    if ((itemKey = TblItem.itemExists(s1)) == 0) {
                        itemKey = TblItem.add(s1, "", "", "Delivered", s2, deliveryKey);
                        LOG.log(Level.FINE, "Added ''{0}'' (in {1}) from delivery", new Object[]{s1,s2});
                        newItems++;
                    } else {
                        LOG.log(Level.FINE, "Item ''{0}'' (in {1}) was received in a previous delivery", new Object[]{s1,s2});
                    }
                    
                    // add a link between this item and the delivery event
                    TblItemEvent.add(itemKey, eventKey);
                }
            }
        } catch (DirectoryIteratorException | IOException e) {
            throw new AppFatal(e.getMessage());
        }
    }
}
