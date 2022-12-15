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
 * This command annotates the items in a directory. The basic annotation is
 * an event.
 * @author Andrew
 */
public class CmdAnnotate extends SubCommand {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdAnnotate");
    private String desc;        // description of the event
    private Path rootDir;       // root directory containing the objects being annotated
    private int count;          // number of items annotated
    private String status;      // status of the items

    public CmdAnnotate() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        int eventKey;

        database = "jdbc:h2:./trackTransfer";
        count = 0;
        status = null;
        
        config(args);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'Annotate' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -desc <description>: annotation text");
            LOG.info("  -dir <filename>: name of directory holding objects being annotated");
            LOG.info("");
            LOG.info(" Optional:");
            LOG.info("  -db <databaseURL>: URL identifying the database (default 'jdbc:h2:./trackTransfer')");
            LOG.info("  -status <status>: String giving new status (special values 'failed' & 'custody-accepted'");
            LOG.info("  -v: verbose mode: give more details about processing");
            LOG.info("  -d: debug mode: give a lot of details about processing");
            LOG.info("  -help: print this listing");
            LOG.info("");
            return;
        }

        // check necessary fields have been specified
        if (desc == null) {
            throw new AppFatal("Annotation text must be specified (-desc)");
        }
        if (rootDir == null) {
            throw new AppFatal("Directory containing the items being annotated must be specified (-dir)");
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Annotate items");
        LOG.log(Level.INFO, " Database: {0}", database);
        LOG.log(Level.INFO, " Annotationn: {0}", desc);
        if (status != null) {
            LOG.log(Level.INFO, " Status: ''{0}''", status);
        } else {
            LOG.info(" Status not specified (and will not change)");
        }
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
        
        // add the delivery event
        eventKey = TblEvent.add(desc);

        // process items in the root directory
        annotateItems(rootDir, eventKey);

        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " {0} items annotated in ({1}) with event {2}", new Object[]{count, database, eventKey});
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
                    // description of the items to be annotated
                    case "-desc":
                        i++;
                        desc = args[i];
                        i++;
                        break;
                    // status of the items to be annotated
                    case "-status":
                        i++;
                        status = args[i].replace("'", "''");
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
     * Annotate the items in this directory. If the item is, itself, a directory
     * annotateItems() is called recursively.
     * 
     * @param dir the directory being processes
     * @param eventKey the event describing this delivery
     * @throws AppFatal something went fatally wrong
     * @throws SQLException a database problem (should never occur)
     */
    private void annotateItems(Path dir, int eventKey) throws AppFatal, SQLException {
        int itemKey;
        String s1, s2;
        Path p;
        int i;
        
        // go through the items in the directory
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path entry : ds) {
                
                // recurse if the item is a directory
                if (entry.toFile().isDirectory()) {
                    annotateItems(entry, eventKey);
                    
                // otherwise register the item    
                } else {
                    
                    // eliminate quote characters as this causes problems with SQL
                    s1 = entry.getFileName().toString().replace("'", "''");
                    p = entry.toRealPath();
                    i = p.getNameCount();
                    if (i < 2) {
                        s2 = p.toString().replace("'", "''");
                    } else {
                        s2 = p.subpath(0, i-1).toString().replace("'", "''");
                    }
                    
                    // if item exist annotate it, otherwise complain
                    if ((itemKey = TblItem.itemExists(s1)) != 0) {
                        if (status != null) {
                            TblItem.changeStatus(itemKey, status);
                        }
                        TblItemEvent.add(itemKey, eventKey);
                        count++;
                        LOG.log(Level.FINE, "Annotated ''{0}'' (in {1})", new Object[]{s1,s2});
                    } else {
                        LOG.log(Level.WARNING, "Failed annotating ''{0}'' (in {1}) as not in database", new Object[]{s1,s2});
                    }
                }
            }
        } catch (DirectoryIteratorException | IOException e) {
            throw new AppFatal(e.getMessage());
        }
    }
}
