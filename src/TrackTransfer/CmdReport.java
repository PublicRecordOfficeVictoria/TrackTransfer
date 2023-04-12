package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This command generates a Report on the Items in a Transfer. Currently five
 * reports are available:
 * 1) A full report of all received Items, Instances, and Events
 * 2) A report listing all Items that have specific Keywords set
 * 3) A report listing all Items for which custody has been accepted
 * 4) A report listing all Items that have been abandoned
 * 5) A report listing all Items for which processing is incomplete
 * 
 * The reports can be generated as human readable text, or a CSV/TSV file. The
 * format is automatically selected depending on the requested file extension
 * (.txt = human readable text, .csv = CSV, .tsv = TSV
 *
 * @author Andrew Waugh
 */
public class CmdReport extends Command {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdAnnotate");
    private Path outputFile;      // report file
    private ReportType type;          // report requested
    private ArrayList<String> keywords; // keywords requested
    private final String usage = "[-db <databaseURL>] -o <file> [-v] [-d] [-help]";

    public CmdReport() throws AppFatal {
        super();
    }

    /**
     * List of Reports available
     */
    public enum ReportType {
        COMPLETE,       // all items and instances
        KEYWORD,        // all items with specific keyword set
        CUSTODY_ACCEPTED, // all items for which custody has been accepted
        ABANDONED,      // all items which have been abandoned
        INCOMPLETE      // all items for which processing is incomplete
    }
    
    /**
     * Generate a report.
     * 
     * @param database database to connect to (may be null)
     * @param type the type of report to generate
     * @param keywords keywords to report on
     * @param format the required output format
     * @param outputFile where to put the generated report
     * @throws AppFatal an internal error occurred
     * @throws AppError an external (user) error occurred
     * @throws SQLException an SQL error occurred
     */
    public void generateReport(String database, ReportType type, ArrayList<String> keywords,  Path outputFile) throws AppFatal, AppError, SQLException {
        this.database = database;
        this.type = type;
        this.keywords = keywords;
        this.outputFile = outputFile;
        
        testParameters();
        doIt();
    }

    public void generateReport(String args[]) throws AppFatal, AppError, SQLException {
        type = ReportType.COMPLETE;
        keywords = new ArrayList<>();

        config(args, usage);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'Report' command line arguments:");
            LOG.info(" Reports available:");
            LOG.info("  -complete: full report of all the items and instances (default)");
            LOG.info("  -keyword <keyword>: report of all items that have the keyword set");
            LOG.info("  -custody-accepted: report of all items for which custody has been accepted");
            LOG.info("  -abandoned: report of all items which have been abandoned");
            LOG.info("  -incomplete: report of all items for which processing is incomplete");
            LOG.info("");
            LOG.info(" Mandatory:");
            LOG.info("  -o <filename>: output file for the report");
            LOG.info("");
            LOG.info(" Optional:");
            LOG.info("  -db <database>: Database name (default based on .mv.db file in current working directory)");
            LOG.info("  -tsv: Generate report as a TSV file (default is plain text)");
            LOG.info("  -csv: Generate report as a CSV file (default is plain text)");
            genericHelp();
            return;
        }

        // check necessary fields have been specified
        testParameters();

        // say what we are doing
        LOG.info("Requested:");
        switch (type) {
            case COMPLETE:
                LOG.info(" Generate Complete report");
                break;
            case KEYWORD:
                LOG.log(Level.INFO, " Generate report for keywords: {0}", keywords);
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
        
        doIt();
        
    }
    
    private void doIt() throws AppFatal, AppError, SQLException {
        Report report;

        // open the output file for writing
        try {

            // connect to the database and create the tables
            connectDB();
            
            // get transfer information
            ResultSet transfer = TblTransfer.query("DESC", null, null);
            String transferDesc = TblTransfer.getDescription(transfer);
            System.out.println("***"+transferDesc);

            //System.out.println(TblItem.printTable());
            //System.out.println(TblInstance.printTable());
            //System.out.println(TblInstanceEvent.printTable());
            //System.out.println(TblEvent.printTable());
            switch (type) {
                case COMPLETE:
                    report = new RptComplete();
                    ((RptComplete) report).generate(outputFile);
                    break;
                case KEYWORD:
                    report = new RptOnItems();
                    ((RptOnItems) report).generate(outputFile, "with keywords", keywords, "FILENAME");
                    break;
                case CUSTODY_ACCEPTED:
                    report = new RptOnItems();
                    keywords.clear();
                    keywords.add("Custody-accepted");
                    ((RptOnItems) report).generate(outputFile, "with status Custody Accepted", keywords, "FILENAME");
                    break;
                case ABANDONED:
                    report = new RptOnItems();
                    keywords.clear();
                    keywords.add("Abandoned");
                    ((RptOnItems) report).generate(outputFile, "with status Abandoned", keywords, "FILENAME");
                    break;
                case INCOMPLETE:
                    report = new RptOnItems();
                    keywords.clear();
                    keywords.add("Incomplete");
                    ((RptOnItems) report).generate(outputFile, "for which processing is incomplete", keywords, "FILENAME");
                    break;
                default:
                    LOG.info(" Requested to generate an unknown type of report");
                    break;
            }

            // for each item, list the events
            disconnectDB();
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
            // output file
            case "-o":
                i++;
                outputFile = Paths.get(args[i]);
                i++;
                j = 2;
                break;
            // complete report of all items/instances/events
            case "-complete":
                type = ReportType.COMPLETE;
                i++;
                j = 1;
                break;
                // report on keywords
            case "-keyword":
                type = ReportType.KEYWORD;
                i++;
                keywords.add(args[i]);
                i++;
                j = 2;
                break;
            // report of all items abandoned
            case "-abandoned":
                type = ReportType.ABANDONED;
                i++;
                j = 1;
                break;
            // report of all items for which custody was accepted
            case "-custody-accepted":
                type = ReportType.CUSTODY_ACCEPTED;
                i++;
                j = 1;
                break;
            // complete report of all items that are incomplete
            case "-incomplete":
                type = ReportType.INCOMPLETE;
                i++;
                j = 1;
                break;
            // otherwise complain
            default:
                j = 0;
        }
        return j;
    }
    
    private void testParameters() throws AppFatal, AppError {
        if (outputFile == null) {
            throw new AppError("Output file is not specified (-o)");
        }
        if (type == ReportType.KEYWORD && keywords == null) {
            throw new AppError("No keywords specified (-keyword)");
        }
    }
}
