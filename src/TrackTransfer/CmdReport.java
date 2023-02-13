package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This command generates a report on the items in a transfer
 *
 * @author Andrew
 */
public class CmdReport extends SubCommand {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdAnnotate");
    private String database;    // database being connected to (may be null)
    private Path outputFile;       // report file
    private Reports reportReq;      // report requested
    private String usage = "[-db <databaseURL>] -o <file> [-v] [-d] [-help]";

    public CmdReport() throws AppFatal {
        super();
    }

    private enum Reports {
        COMPLETE, // all items and instances
        CUSTODY_ACCEPTED, // all items for which custody has been accepted
        ABANDONED, // all items which have been abandoned
        INCOMPLETE  // all items for which processing is incomplete
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        FileOutputStream fos;
        OutputStreamWriter osw;
        BufferedWriter bw;

        reportReq = Reports.COMPLETE;

        config(args, usage);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'Report' command line arguments:");
            LOG.info(" Reports available:");
            LOG.info("  -complete: full report of all the items and instances (default)");
            LOG.info("  -custody-accepted: report of all items for which custody has been accepted");
            LOG.info("  -abandoned: report of all items which have been abandoned");
            LOG.info("  -incomplete: report of all items for which processing is incomplete");
            LOG.info("");
            LOG.info(" Mandatory:");
            LOG.info("  -o <filename>: output file for the report");
            LOG.info("");
            LOG.info(" Optional:");
            LOG.info("  -db <database>: Database name (default based on .mv.db file in current working directory)");
            genericHelp();
            return;
        }

        // check necessary fields have been specified
        if (outputFile == null) {
            throw new AppFatal("Output file is not specified (-o)");
        }

        // say what we are doing
        LOG.info("Requested:");
        switch (reportReq) {
            case COMPLETE:
                LOG.info(" Generate Complete report");
                break;
            case CUSTODY_ACCEPTED:
                LOG.info(" Generate Custody Accepted report");
                break;
            case ABANDONED:
                LOG.info(" Generate Abandoned report");
                break;
            case INCOMPLETE:
                LOG.info(" Generate Items not finalised report");
                break;
            default:
                LOG.info(" Generate Unknown report");
                break;
        }
        LOG.log(Level.INFO, " Database: {0}", database == null ? "Derived from .mv.db filename" : database);
        LOG.log(Level.INFO, " Report: {0}", outputFile.toString());
        genericStatus();

        // open the output file for writing
        try {
            fos = new FileOutputStream(outputFile.toFile());
            osw = new OutputStreamWriter(fos, "UTF-8");
            bw = new BufferedWriter(osw);
            bw.append("Report of all items\n\n");

            // connect to the database and create the tables
            database = connectDB(database);

            //System.out.println(TblItem.printTable());
            //System.out.println(TblInstance.printTable());
            //System.out.println(TblInstanceEvent.printTable());
            //System.out.println(TblEvent.printTable());
            switch (reportReq) {
                case COMPLETE:
                    completeReport(bw);
                    break;
                case CUSTODY_ACCEPTED:
                    custodyAcceptedReport(bw);
                    break;
                case ABANDONED:
                    abandonedReport(bw);
                    break;
                case INCOMPLETE:
                    incompleteReport(bw);
                    break;
                default:
                    LOG.info(" Generate Unknown report");
                    break;
            }

            // for each item, list the events
            disconnectDB();

            bw.append("End of Report");

            bw.close();
            osw.close();
            fos.close();
        } catch (IOException ioe) {
            throw new AppError(ioe.getMessage());
        }

        // acknowledge creation
        LOG.log(Level.INFO, "Report generated from ({0}) to ''{1}''", new Object[]{database, outputFile.toString()});
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
            // output file
            case "-o":
                i++;
                outputFile = Paths.get(args[i]);
                i++;
                j = 2;
                break;
            // complete report of all items/instances/events
            case "-complete":
                reportReq = Reports.COMPLETE;
                i++;
                j = 1;
                break;
            // report of all items abandoned
            case "-abandoned":
                reportReq = Reports.ABANDONED;
                i++;
                j = 1;
                break;
            // report of all items for which custody was accepted
            case "-custody-accepted":
                reportReq = Reports.CUSTODY_ACCEPTED;
                i++;
                j = 1;
                break;
            // complete report of all items that are incomplete
            case "-incomplete":
                reportReq = Reports.INCOMPLETE;
                i++;
                j = 1;
                break;
            // otherwise complain
            default:
                j = 0;
        }
        return j;
    }

    /**
     * Generate a complete report of all items received, all instances for each
     * item, and all events for each instance.
     *
     * @param w
     * @throws SQLException
     * @throws IOException
     */
    private void completeReport(Writer w) throws SQLException, IOException {
        int itemKey, instanceKey;
        ResultSet items, instances, events;
        int i;

        w.append("COMPLETE REPORT (all Items/Instances/Events)\n");
        w.append("Run \n");
        w.append("\n");

        // go through the items
        i = 0;
        items = TblItem.query("*", null, "FILENAME");
        while (items.next()) {
            i++;
            if (i % 100 == 0) {
                System.out.println(i);
            }
            w.append(TblItem.reportItem(items));
            w.append("\n");
            itemKey = TblItem.getItemId(items);

            // get instances of this item
            instances = TblInstance.query("*", "ITEM_ID=" + itemKey, "ITEM_ID");
            // instances = SQL.query("select * from INSTANCE where ITEM_ID="+itemKey+" ORDER BY ITEM_ID");
            while (instances.next()) {
                w.append("  ");
                w.append(TblInstance.reportInstance(instances));
                w.append("\n");
                instanceKey = TblInstance.getInstanceId(instances);

                // get events related to this instance
                events = SQL.query("INSTANCE_EVENT join EVENT on INSTANCE_EVENT.EVENT_ID=EVENT.EVENT_ID", "*", "INSTANCE_EVENT.INSTANCE_ID=" + instanceKey, "EVENT_ID");
                while (events.next()) {
                    w.append("    ");
                    w.append(TblEvent.reportEvent(events));
                    w.append("\n");
                }
            }
        }
    }

    private void custodyAcceptedReport(Writer w) throws SQLException, IOException {
        ResultSet items;
        int i;

        w.append("Custody Accepted report\n");
        w.append("Run \n");
        w.append("\n");

        // go through the items
        i = 0;
        items = TblItem.query("*", "STATUS='Custody-Accepted'", "FILENAME");
        while (items.next()) {
            i++;
            if (i % 100 == 0) {
                System.out.println(i);
            }
            w.append(TblItem.reportItem(items));
            w.append("\n");
        }
    }

    private void abandonedReport(Writer w) throws SQLException, IOException {
        ResultSet items;
        int i;

        w.append("Items abandoned report\n");
        w.append("Run \n");
        w.append("\n");

        // go through the items
        i = 0;
        items = TblItem.query("*", "STATUS='Abandoned'", "FILENAME");
        while (items.next()) {
            i++;
            if (i % 100 == 0) {
                System.out.println(i);
            }
            w.append(TblItem.reportItem(items));
            w.append("\n");
        }

    }

    private void incompleteReport(Writer w) throws SQLException, IOException {
        ResultSet items;
        int i;

        w.append("Items unfinished report\n");
        w.append("Run \n");
        w.append("\n");

        // go through the items
        i = 0;
        items = TblItem.query("*", "IS_FINALISED=FALSE", "FILENAME");
        while (items.next()) {
            i++;
            if (i % 100 == 0) {
                System.out.println(i);
            }
            w.append(TblItem.reportItem(items));
            w.append("\n");
        }

    }
}
