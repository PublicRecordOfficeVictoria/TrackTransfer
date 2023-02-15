package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * This command annotates the items in a directory. The basic annotation is an
 * event.
 *
 * @author Andrew
 */
public class CmdAnnotateFromFile extends SubCommand {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdLoadFile");
    private Path file;          // file to process for Item names & condition
    private int skip;           // number of lines in the header to skip
    private List<MatchPattern> patterns; // patterns to match agains C/TSV files
    private int fileColumn;     // column in which to find the filename
    private boolean csv;        // true if files are separated by commas rather than tabs
    private String database;    // database being connected to (may be null)
    private String desc;        // description of the event
    private int count;          // number of items annotated
    private String status;      // status of the items
    private boolean isFinalised;// true if the status implies that the item has been finalised (i.e. custody accepted or abandoned)
    private String usage = "[-db <database>] [-skip <count>] [-csv] [-tsv] [-pattern <pattern>] [-desc <text>] [-status <text>] [-i] file [-v] [-d] [-help]";

    /**
     * Private class that stores a pattern to be matched against lines in files
     * Each pattern consists of a column number (starting at 0) and a regular
     * expression.
     */
    private static class MatchPattern {

        int column;             // column to match against
        Pattern pattern;        // pattern to match in column

        /**
         * Creator - given a string containing a column number and a pattern.
         *
         * @param column Column number (as a string). Must be > -1;
         * @param pattern regular expression
         * @throws AppError
         */
        public MatchPattern(String column, String pattern) throws AppError {
            assert column != null;
            assert pattern != null;
            try {
                this.column = Integer.parseInt(column);
            } catch (NumberFormatException nfe) {
                throw new AppError("Column number in pattern was not an integer: " + nfe.getMessage());
            }
            if (this.column < 0) {
                throw new AppError("Column number in pattern was not positive or zero: " + this.column);
            }
            try {
                this.pattern = Pattern.compile(pattern);
            } catch (PatternSyntaxException pse) {
                throw new AppError("Pattern failed to compile: " + pse.getMessage());
            }
            // System.out.println("Pattern: Col=" + this.column + " Pattern='" + pattern + "'");
        }

        /**
         * Parse a string containing a sequence of patterns. Each pattern is
         * separated by ',', and each pattern has a leading column number and
         * then a regular expression separated by a '='
         *
         * @param s String containing the patterns
         * @return
         * @throws AppError
         */
        public static List<MatchPattern> parse(String s) throws AppError {
            ArrayList<MatchPattern> l = new ArrayList<>();
            MatchPattern mp;
            String[] patterns;
            String[] components;
            int i;

            patterns = s.split(",");
            for (i = 0; i < patterns.length; i++) {
                components = patterns[i].split("=");
                if (components.length != 2) {
                    throw new AppError("Pattern does not match <column>=<pattern>: '" + patterns[i] + "'");
                }
                if (components[1].startsWith("\"")) {
                    components[1] = components[1].substring(1);
                }
                if (components[1].endsWith("\"")) {
                    components[1] = components[1].substring(0, components[1].length() - 1);
                }
                mp = new MatchPattern(components[0], components[1]);
                l.add(mp);
            }
            return l;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            
            sb.append("Column ");
            sb.append(column);
            sb.append(" = '");
            sb.append(pattern.toString());
            sb.append("'");
            return sb.toString();
        }
    }

    public CmdAnnotateFromFile() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        int eventKey;

        skip = 0;
        fileColumn = -1;
        count = 0;
        status = null;
        isFinalised = false;
        csv = false;
        patterns = null;

        config(args, usage);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'LoadFile' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -in <filename>: name of file containing the filenames");
            LOG.info("  -filename <column>: column containing filename (Item name). First column = 0");
            LOG.info("");
            LOG.info(" Optional:");
            LOG.info("  -db <database>: Database name (default based on .mv.db file in current working directory)");
            LOG.info("  -skip <count>: Number of lines at head of file to skip");
            LOG.info("  -pattern <pattern>: columns/patterns to match against. Formant <columnNumber>=<regex>,... Default always select");
            LOG.info("  -tsv: file is a TSV file (default)");
            LOG.info("  -csv: file is a CSV file");
            LOG.info("  -status <status>: String giving new status (special values 'abandoned' & 'custody-accepted'");
            LOG.info("  -desc <description>: annotation text");
            LOG.info("  -custody-accepted: equivalent to '-status Custody-Accepted'");
            LOG.info("  -abandoned: equivalent to '-status Abandoned'");
            genericHelp();
            LOG.info("");
            LOG.info("One or both of -status and -desc must be present");
            return;
        }

        // check necessary fields have been specified
        if (file == null) {
            throw new AppError("Must specify an input file name (-in)");
        }
        if (fileColumn < 0) {
            throw new AppError("Must specify a column in which the filename (Item name) is to be found (-filename)");
        }
        if (skip < 0) {
            throw new AppError("Skip count must be a positive number (-skip)");
        }
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

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Annotate from file");
        LOG.log(Level.INFO, " Database: {0}", database == null ? "Derived from .mv.db filename" : database);
        LOG.log(Level.INFO, " File to process: {0}", file.toString());
        if (csv) {
            LOG.info(" Columns in file are separated by commas (CSV file)");
        } else {
            LOG.info(" Columns in file are separated by tabs (TSV file)");
        }
        LOG.log(Level.INFO, " Lines to skip at start of file: {0}", skip);
        LOG.log(Level.INFO, " Patterns to match against: {0}", patterns==null?"Match all lines":patterns.toString());
        LOG.log(Level.INFO, " Item name (filename) column: {0}", fileColumn);
        LOG.log(Level.INFO, " Annotationn: {0}", desc);
        if (status != null) {
            LOG.log(Level.INFO, " Status: ''{0}''", status);
        } else {
            LOG.info(" Status not specified (and will not change)");
        }
        if (isFinalised) {
            LOG.log(Level.INFO, " Item automatically finalised (status = custody-accepted or abandoned");
        }
        genericStatus();

        // check if the root directory is a directory and exists
        if (!file.toFile().exists()) {
            throw new AppError("Load File: file '" + file.toString() + "' does not exist");
        }
        if (!file.toFile().isFile()) {
            throw new AppError("Load File: file '" + file.toString() + "' is not a directory");
        }

        // connect to the database and create the tables
        database = connectDB(database);

        // add the delivery event
        eventKey = TblEvent.add(desc);

        // process items listed in the file
        processFile(file, patterns, eventKey);

        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " {0} items annotated in ({1}) with event {2}", new Object[]{count, database, eventKey});
    }

    /**
     * The specific configuration for this command.
     *
     * @param args
     * @param i
     * @return
     * @throws AppError
     * @throws ArrayIndexOutOfBoundsException
     */
    @Override
    int specificConfig(String[] args, int i) throws AppError, ArrayIndexOutOfBoundsException {
        int j;

        switch (args[i].toLowerCase()) {
            // description of the items to be annotated
            case "-desc":
                i++;
                desc = args[i];
                i++;
                j = 2;
                break;
            // status of the items to be annotated
            case "-status":
                i++;
                status = args[i].replace("'", "''");
                i++;
                j = 2;
                break;
            // custody has been accepted of the items
            case "-custody-accepted":
                i++;
                status = "Custody-Accepted";
                j = 1;
                break;
            // items have been abandoned
            case "-abandoned":
                i++;
                status = "Abandoned";
                j = 1;
                break;
            // directory that contains the items in the delivery
            case "-in":
                i++;
                file = Paths.get(args[i]);
                i++;
                j = 2;
                break;
            // number of lines to skip
            case "-skip":
                i++;
                try {
                    skip = Integer.parseInt(args[i]);
                } catch (NumberFormatException nfe) {
                    throw new AppError("Failed converting skip count to an integer: " + nfe.getMessage());
                }
                i++;
                j = 2;
                break;
            // number of lines to patterns to match
            case "-pattern":
                i++;
                patterns = MatchPattern.parse(args[i]);
                i++;
                j = 2;
                break;
            // column in which to find the file name
            case "-filename":
                i++;
                try {
                    fileColumn = Integer.parseInt(args[i]);
                } catch (NumberFormatException nfe) {
                    throw new AppError("Failed converting file name column to an integer: " + nfe.getMessage());
                }
                i++;
                j = 2;
                break;
            // file is a CSV file
            case "-csv":
                i++;
                csv = true;
                j = 1;
                break;
            // file is a TSV file
            case "-tsv":
                i++;
                csv = false;
                j = 1;
                break;
            // otherwise complain
            default:
                j = 0;
        }
        return j;
    }

    /**
     * Process the file. It is assumed that the file is a CSV or TSV file with
     * one Item specified per line. The minimum is a specification as to which
     * column will contain the filename of the Item. An option is a boolean test
     * over information in the other columns to determine if the line is
     * successful. Optionally lines may be skipped at the head of the file.
     *
     * @param dir the directory being processes
     * @param eventKey the event describing this delivery
     * @throws AppFatal something went fatally wrong
     * @throws SQLException a database problem (should never occur)
     */
    private void processFile(Path file, List<MatchPattern> patterns, int eventKey) throws AppError, AppFatal, SQLException {
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;
        String line;
        String[] tokens;

        // read control file
        try {
            fis = new FileInputStream(file.toFile().toString());
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);

            while ((line = br.readLine()) != null) {
                // optionally skip the first 'skip' lines
                if (skip > 0) {
                    skip--;
                    continue;
                }

                // split the CSV line into columns
                if (csv) {
                    tokens = line.split(",");
                } else {
                    tokens = line.split("\t");
                }

                /*
                for (i = 0; i < tokens.length; i++) {
                    System.out.print("(" + i + ") '" + tokens[i] + "' ");
                }
                System.out.println("");
                */

                // annotate the specified Item if the line matches the pattern
                if (patterns == null || test(patterns, tokens)) {
                    annotate(tokens, eventKey);
                    count++;
                }
            }
            br.close();
            isr.close();
            fis.close();
        } catch (FileNotFoundException e) {
            throw new AppError("Failed to open control file '" + file.toString() + "'" + e.toString());
        } catch (IOException e) {
            throw new AppError("Failed reading the control file '" + file.toString() + "'" + e.toString());
        }
    }

    /**
     * Test the line against the specified pattern. If any patterns in the
     * pattern do not match, return false. If they all match, return true.
     *
     * @param patterns the set of patterns to match against the line
     * @param tokens the line to be matched
     * @return true if all the patterns match
     */
    private boolean test(List<MatchPattern> patterns, String[] tokens) throws AppError {
        int i;
        MatchPattern mp;
        Matcher m;

        for (i = 0; i < patterns.size(); i++) {
            mp = patterns.get(i);
            if (mp.column > tokens.length) {
                continue;
            }
            m = mp.pattern.matcher(tokens[mp.column]);
            if (!m.matches()) {
                return false;
            }
            // System.out.println("Column "+mp.column+" is '"+tokens[mp.column]);
        }
        return true;
    }

    /**
     * Get the file name, find the Item, and annotate it.
     *
     * @param tokens
     * @throws AppError
     */
    private void annotate(String[] tokens, int eventKey) throws AppError, AppFatal, SQLException {
        if (tokens.length < fileColumn + 1) {
            throw new AppError("Line does not contain enough columns to have the file name");
        }
        CmdAnnotate.annotateItem(tokens[fileColumn], eventKey, status, isFinalised);
    }
}
