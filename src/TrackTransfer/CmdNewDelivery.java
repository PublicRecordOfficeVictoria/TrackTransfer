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
    private String database;    // database being connected to (may be null)
    private String desc;         // description of this delivery
    private Path rootDir;        // root directory containing the objects being delivered
    private int noFiles;         // total number of files found in delivery
    private int numNotRecords;   // total number of files that are not instances of records found
    private int numRecords;      // number of instances of records found
    private boolean supersedePrevious; // if true, any duplicates are assumed to supersede the previous instance
    private int receivedEvent;   // event documenting the receipt of the instance
    private int newItemEvent;    // event documenting the first receipt in a delivery
    private int newRecordEvent;  // event stating that instances was received as a new record
    private int supersededEvent; // event stating that this instance replaced a previous instance
    private int supersedesEvent; // event stating that this instance supersedes a previous instance
    private int replacedByDuplNewDeliveryEvent; // event stating that this instance duplicates another instance
    private int replacesDuplPrevDeliveryEvent; // event stating that this instance duplicates an instance in a previous delivery
    private int replacedByDuplThisDeliveryEvent; // event stating that this instance was duplicated in a delivery
    private int replacesDuplThisDeliveryEvent; // event stating that this instance duplicates an instance in this delivery
    private int notRecordEvent;  // event stating that this instance was judged to be not a record
    private int tooLateEvent;    // event stating that this instance was received after record had had custody accepted

    public CmdNewDelivery() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        int transferKey, deliveryKey, eventKey;
        ResultSet rs;
        
        noFiles = 0;
        numNotRecords = 0;
        numRecords = 0;
        supersedePrevious = true;
        receivedEvent = 0;
        newRecordEvent = 0;
        supersededEvent = 0;
        replacedByDuplNewDeliveryEvent = 0;
        notRecordEvent = 0;
        tooLateEvent = 0;

        config(args);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'New delivery' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -desc <description>: text describing this delivery");
            LOG.info("  -dir <filename>: name of directory holding objects being delivered");
            LOG.info("");
            LOG.info(" Optional:");
            LOG.info("  -ignore-dups: any duplicate records are to be ignored (default is to replace)");
            LOG.info("  -db <databaseURL>: URL identifying the database (default 'jdbc:h2:./trackTransfer')");
            LOG.info("  -v: verbose mode: give more details about processing");
            LOG.info("  -d: debug mode: give a lot of details about processing");
            LOG.info("  -help: print this listing");
            LOG.info("");
            return;
        }

        // check necessary fields have been specified
        if (desc == null) {
            throw new AppError("Delivery description must be specified (-desc)");
        }
        if (rootDir == null) {
            throw new AppError("Directory containing the items being delivered must be specified (-dir)");
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Register a new delivery");
        LOG.log(Level.INFO, " Database: {0}", database==null?"Derived from .mv.db filename":database);
        LOG.log(Level.INFO, " Description: {0}", desc);
        LOG.log(Level.INFO, " Directory of items: {0}", rootDir.toString());
        if (supersedePrevious) {
            LOG.info(" Any records that duplicate those in previous deliveries will supersede the previous instance");
        } else {
            LOG.info(" Any records that duplicate those in previous deliveries will be ignored as duplicates");
        }
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

        // get the key for the one transfer
        transferKey = 0;
        rs = TblTransfer.query("TRANSFER_ID", null, null);
        while (rs.next()) {
            transferKey = TblTransfer.getTransferId(rs);
        }

        // add the details about the delivery
        deliveryKey = TblDelivery.add(transferKey, desc, rootDir);

        // add the delivery event
        eventKey = TblEvent.add(desc);

        // process instances in the root directory
        registerInstances(deliveryKey, desc, rootDir, eventKey);

        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " Delivery added to ''{0}''; found: {1} (records: {2}, not records: {3})", new Object[]{database, noFiles, numRecords, numNotRecords});
        LOG.log(Level.INFO, " Delivery row (key={0})", deliveryKey);
    }

    public void config(String args[]) throws AppError {
        String usage = "[-db <databaseURL>] -desc <text> -dir <directory>";
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
                    // directory that contains the items in the delivery
                    case "-ignore-dups":
                        i++;
                        supersedePrevious = false;
                        break;
                    // otherwise complain
                    default:
                        throw new AppError("Unrecognised argument '" + args[i] + "'. Usage: " + usage);
                }
            }
        } catch (ArrayIndexOutOfBoundsException ae) {
            throw new AppError("Missing argument. Usage: " + usage);
        }
    }

    /**
     * Register the item instances in this directory. If the item is, itself, a
     * directory registerItems() is called recursively.
     *
     * @param deliveryKey the delivery this item belongs to
     * @param desc the description of the delivery
     * @param dir the directory being processes
     * @param eventKey the event describing this delivery
     * @throws AppFatal something went fatally wrong
     * @throws SQLException a database problem (should never occur)
     */
    private void registerInstances(int deliveryKey, String desc, Path dir, int eventKey) throws AppFatal, SQLException {

        // go through the items in the directory
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path entry : ds) {

                // recurse if the item is a directory
                if (entry.toFile().isDirectory()) {
                    registerInstances(deliveryKey, desc, entry, eventKey);

                    // otherwise register the item
                } else {
                    noFiles++;
                    registerInstance(entry, deliveryKey, desc, eventKey);
                }
            }
        } catch (DirectoryIteratorException | IOException e) {
            throw new AppFatal(e.getMessage());
        }
    }

    /**
     * Register an item instance.
     *
     * @param entry the instance in the delivery
     * @param deliveryKey the delivery this item belongs to
     * @param deliveryDesc the description of the delivery
     * @param eventKey the event describing this delivery
     * @throws AppFatal something went fatally wrong
     * @throws SQLException a database problem (should never occur)
     */
    private void registerInstance(Path entry, int deliveryKey, String deliveryDesc, int eventKey) throws AppFatal, SQLException {
        int instanceKey, itemKey, prevInstanceKey;
        String filename;
        boolean isRecord, isFinalised;
        ResultSet rsItem, rsPrevInstance;

        // get identifier
        filename = entry.getFileName().toString();

        // determine if this instance is not a record
        isRecord = true;
        /* ignore this for the moment to assist in initial testing
        if (!filename.toLowerCase().endsWith(".veo.zip") && !filename.toLowerCase().endsWith(".veo")) {
            LOG.log(Level.FINE, "Eliminated ignoreNotRecord file ''{0}''", entry.toString());
            numNotRecords++;
            isRecord = false;
        }
         */

        // add instance to Instance table
        instanceKey = TblInstance.add(deliveryKey, 0, entry.toString(), false, 0);
        assert instanceKey != 0;

        // if we have already seen this item, this new instance must either
        // supersede or duplicate an earlier instance (duplicates may either be
        // within this delivery or an earlier delivery). A special case is where
        // the item has already been finalised.
        rsItem = TblItem.findItem(filename, null);
        if (rsItem.next()) {
            itemKey = TblItem.getItemId(rsItem);
            isFinalised = TblItem.isFinalised(rsItem);

            // Duplicate handling. It's a duplicate if we have already seen
            // this instance in this delivery, or if we saw it in a previous
            // delivery & the user hasn't said that the instances supersede
            // previous ones.
            prevInstanceKey = TblItem.getActiveInstanceId(rsItem);
            assert prevInstanceKey != 0;
            rsPrevInstance = TblInstance.getInstance(prevInstanceKey);
            assert rsPrevInstance != null;
            rsPrevInstance.next();

            // If the item is not finalised, mark the previous instance as
            // duplicated or superseded. If it is finalised, this instances
            // will be marked as 'too late' further down
            if (!isFinalised) {
                if (TblInstance.getDeliveryId(rsPrevInstance) == deliveryKey) { // duplicate in this delivery
                    TblInstance.setIsDuplicated(prevInstanceKey);
                    if (replacedByDuplThisDeliveryEvent == 0) {
                        replacedByDuplThisDeliveryEvent = TblEvent.add("Instance replaced by a duplicate received in this delivery");
                    }
                    assert replacedByDuplThisDeliveryEvent != 0;
                    TblInstanceEvent.add(prevInstanceKey, replacedByDuplThisDeliveryEvent);
                    if (replacesDuplThisDeliveryEvent == 0) {
                        replacesDuplThisDeliveryEvent = TblEvent.add("Instance replaces duplicate received in the same delivery");
                    }
                    assert replacesDuplThisDeliveryEvent != 0;
                    TblInstanceEvent.add(instanceKey, replacesDuplThisDeliveryEvent);
                    LOG.log(Level.WARNING, "Record ''{0}'' ({1}) already appears in this delivery", new Object[]{filename, entry.toString()});
                } else if (!supersedePrevious) { // duplicate in a previous delivery
                    TblInstance.setIsDuplicated(prevInstanceKey);
                    if (replacedByDuplNewDeliveryEvent == 0) {
                        replacedByDuplNewDeliveryEvent = TblEvent.add("Instance replaced by a duplicate received in a later delivery (" + desc + ")");
                    }
                    assert replacedByDuplNewDeliveryEvent != 0;
                    TblInstanceEvent.add(prevInstanceKey, replacedByDuplNewDeliveryEvent);
                    if (replacesDuplPrevDeliveryEvent == 0) {
                        replacedByDuplNewDeliveryEvent = TblEvent.add("Instance replaces duplicate received in a previous delivery");
                    }
                    assert replacesDuplPrevDeliveryEvent != 0;
                    TblInstanceEvent.add(instanceKey, replacesDuplPrevDeliveryEvent);
                    LOG.log(Level.WARNING, "Record ''{0}'' ({1}) duplicated instance in previous delivery", new Object[]{filename, entry.toString()});
                } else { //supersedes previous instance
                    TblInstance.setIsSuperseded(prevInstanceKey);
                    if (supersededEvent == 0) {
                        supersededEvent = TblEvent.add("Instance superseded by a duplicate received in a later delivery (" + desc + ")");
                    }
                    assert replacedByDuplNewDeliveryEvent != 0;
                    TblInstanceEvent.add(prevInstanceKey, supersededEvent);
                    if (supersedesEvent == 0) {
                        supersedesEvent = TblEvent.add("Instance supersedes that received in a previous delivery");
                    }
                    assert supersedesEvent != 0;
                    TblInstanceEvent.add(instanceKey, supersedesEvent);
                    LOG.log(Level.WARNING, "Record ''{0}'' ({1}) superseded instance in previous delivery", new Object[]{filename, entry.toString()});
                }
            }
        } else { // seen for the first time, create the item
            LOG.log(Level.FINE, "Created item ''{0}'' from instance ''{1}''", new Object[]{filename, entry.toString()});
            itemKey = TblItem.add(filename, null, null, isRecord, false, "Status", instanceKey);
            isFinalised = false;
            if (newItemEvent == 0) {
                newItemEvent = TblEvent.add("Received for the first time in a delivery (" + desc + ")");
            }
            assert newItemEvent != 0;
            TblInstanceEvent.add(instanceKey, newItemEvent);
        }

        // set item id in the new instance
        TblInstance.setItemId(instanceKey, itemKey);

        // make this instance the current instance
        TblItem.setCurrentInstance(itemKey, instanceKey);

        // is this item already finalised? If so, generate a 'too late' event,
        // otherwise make this instance the active instance
        if (!isFinalised) {
            TblItem.setActiveInstance(itemKey, instanceKey);
        } else {
            if (tooLateEvent == 0) {
                tooLateEvent = TblEvent.add("Record resubmitted in delivery " + deliveryKey + " but custody has already been accepted");
            }
            assert tooLateEvent != 0;
            TblInstanceEvent.add(instanceKey, tooLateEvent);
            LOG.log(Level.WARNING, "Record ''{0}'' ({1}) resubmitted in delivery, but custody has already been accepted", new Object[]{filename, entry.toString()});
        }
    }
}
