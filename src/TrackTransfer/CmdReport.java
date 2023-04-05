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
 * This command generates a report on the items in a transfer
 *
 * @author Andrew
 */
public class CmdReport extends Command {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdAnnotate");
    private Path outputFile;        // report file
    private Reports reportReq;      // report requested
    private ArrayList<String> keywords;      // keywords requested
    private Report.ReportType format;      // format of requested report
    private final String usage = "[-db <databaseURL>] -o <file> [-v] [-d] [-help]";

    public CmdReport() throws AppFatal {
        super();
    }

    private enum Reports {
        COMPLETE,       // all items and instances
        KEYWORD,        // all items with specific keyword set
        CUSTODY_ACCEPTED, // all items for which custody has been accepted
        ABANDONED,      // all items which have been abandoned
        INCOMPLETE      // all items for which processing is incomplete
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        Report report;
        String[] s;

        reportReq = Reports.COMPLETE;
        format = Report.ReportType.TEXT;
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
        if (outputFile == null) {
            throw new AppFatal("Output file is not specified (-o)");
        }

        // say what we are doing
        LOG.info("Requested:");
        switch (reportReq) {
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
        switch (format) {
            case TEXT:
                LOG.info(" Report format: plain text");
                break;
            case CSV:
                LOG.info(" Report format: CSV");
                break;
            case TSV:
                LOG.info(" Report format: TSV");
                break;
            default:
                LOG.info(" Report format: unknown");
                break;
        }
        genericStatus();

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
            switch (reportReq) {
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
                    keywords.add("Custody-accepted");
                    ((RptOnItems) report).generate(outputFile, "with status Custody Accepted", keywords, "FILENAME");
                    break;
                case ABANDONED:
                    report = new RptOnItems();
                    keywords.add("Abandoned");
                    ((RptOnItems) report).generate(outputFile, "with status Abandoned", keywords, "FILENAME");
                    break;
                case INCOMPLETE:
                    report = new RptOnItems();
                    keywords.add("Incomplete");
                    ((RptOnItems) report).generate(outputFile, "for which processing is incomplete", keywords, "FILENAME");
                    break;
                default:
                    LOG.info(" Generate Unknown report");
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
            // TSV report requested
            case "-tsv":
                i++;
                format = Report.ReportType.TSV;
                j = 1;
                break;
            // CSV report requested
            case "-csv":
                i++;
                format = Report.ReportType.CSV;
                j = 1;
                break;
            // complete report of all items/instances/events
            case "-complete":
                reportReq = Reports.COMPLETE;
                i++;
                j = 1;
                break;
                // report on keywords
            case "-keyword":
                reportReq = Reports.KEYWORD;
                i++;
                keywords.add(args[i]);
                i++;
                j = 2;
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
}
