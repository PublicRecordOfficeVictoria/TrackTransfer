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
 * This command annotates the items in a directory. The basic annotation is an
 * event.
 *
 * @author Andrew
 */
public class CmdAnnotate extends SubCommand {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdAnnotate");
    private String database;    // database being connected to (may be null)
    private String desc;        // description of the event
    private Path rootDir;       // root directory containing the objects being annotated
    private int count;          // number of items annotated
    private String status;      // status of the items
    boolean isFinalised;        // true if the status implies that the item has been finalised (i.e. custody accepted or abandoned)

    public CmdAnnotate() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        int eventKey;

        count = 0;
        status = null;
        isFinalised = false;

        config(args);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'Annotate' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -dir <filename>: name of directory holding objects being annotated");
            LOG.info("");
            LOG.info(" Optional:");
            LOG.info("  -db <databaseURL>: URL identifying the database (default 'jdbc:h2:./trackTransfer')");
            LOG.info("  -status <status>: String giving new status (special values 'abandoned' & 'custody-accepted'");
            LOG.info("  -desc <description>: annotation text");
            LOG.info("  -v: verbose mode: give more details about processing");
            LOG.info("  -d: debug mode: give a lot of details about processing");
            LOG.info("  -help: print this listing");
            LOG.info("");
            LOG.info("One or both of -status and -desc must be present");
            return;
        }

        // check necessary fields have been specified
        if (desc == null) {
            if (status != null) {
                desc = "Status changed to '" + status + "'";
            } else {
                throw new AppError("Annotation text must be specified (-desc)");
            }
        }
        if (status != null) {
            switch (status.toLowerCase()) {
                case "custody-accepted":
                case "abandoned":
                    isFinalised = true;
                    desc = desc + "& item automatically finalised";
                    break;
                default:
                    isFinalised = false;
                    break;
            }
        }
        if (rootDir == null) {
            throw new AppFatal("Directory containing the items being annotated must be specified (-dir)");
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Annotate items");
        LOG.log(Level.INFO, " Database: {0}", database==null?"Derived from .mv.db filename":database);
        LOG.log(Level.INFO, " Annotationn: {0}", desc);
        if (status != null) {
            LOG.log(Level.INFO, " Status: ''{0}''", status);
        } else {
            LOG.info(" Status not specified (and will not change)");
        }
        if (isFinalised) {
            LOG.log(Level.INFO, " Item automatically finalised (status = custody-accepted or abandoned");
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
            throw new AppError("New Delivery: directory '" + rootDir.toString() + "' does not exist");
        }
        if (!rootDir.toFile().isDirectory()) {
            throw new AppError("New Delivery: directory '" + rootDir.toString() + "' is not a directory");
        }

        // connect to the database and create the tables
        database = connectDB(database);

        // add the delivery event
        eventKey = TblEvent.add(desc);

        // process items in the root directory
        annotateItems(rootDir, eventKey);

        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " {0} items annotated in ({1}) with event {2}", new Object[]{count, database, eventKey});
    }

    public void config(String args[]) throws AppError {
        String usage = "[-db <databaseURL>] [-desc <text>] [-status <text>] [-final] [-abandon] -dir <directory> [-v] [-d] [-help]";
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
                    // custody has been accepted of the items
                    case "-custody-accepted":
                        i++;
                        status = "Custody-Accepted";
                        break;
                    // items have been abandoned
                    case "-abandoned":
                        i++;
                        status = "Abandoned";
                        break;
                    // directory that contains the items in the delivery
                    case "-dir":
                        i++;
                        rootDir = Paths.get(args[i]);
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
        int itemKey, instanceKey;
        ResultSet rsItem;
        String s1;

        // go through the items in the directory
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path entry : ds) {

                // recurse if the item is a directory
                if (entry.toFile().isDirectory()) {
                    annotateItems(entry, eventKey);

                    // otherwise change the status and/or description   
                } else {

                    // eliminate quote characters as this causes problems with SQL
                    s1 = getFileName(entry);

                    // if item exist annotate it, otherwise complain
                    rsItem = TblItem.findItem(s1, null);
                    if (rsItem.next()) {
                        itemKey = TblItem.getItemId(rsItem);
                        assert itemKey != 0;
                        if (status != null) {
                            TblItem.setStatus(itemKey, status, isFinalised);
                        }
                        instanceKey = TblItem.getActiveInstanceId(rsItem);
                        assert instanceKey != 0;
                        TblInstanceEvent.add(instanceKey, eventKey);
                        count++;
                        LOG.log(Level.FINE, "Annotated ''{0}''", new Object[]{s1});
                    } else {
                        LOG.log(Level.WARNING, "Failed annotating ''{0}'' as it was not in the database", new Object[]{s1});
                    }
                }
            }
        } catch (DirectoryIteratorException | IOException e) {
            throw new AppFatal(e.getMessage());
        }
    }

    /**
     * Get a file name from a path. Suppress the final '.lnk' in a Windows short
     * cut. Replace single quotes with two quotes to be SQL safe.
     *
     * @param p
     * @return
     */
    private String getFileName(Path p) {
        String s;

        assert p != null;
        s = p.getFileName().toString().trim();
        if (s.toLowerCase().endsWith(".lnk")) {
            s = s.substring(0, s.length() - 4);
        } else if (s.toLowerCase().endsWith(" - shortcut")) {
            s = s.substring(0, s.length() - 11);
        }
        s = s.replace("'", "''");
        return s;
    }
}
