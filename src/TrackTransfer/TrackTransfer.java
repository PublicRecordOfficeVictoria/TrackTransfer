/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import VERSCommon.VEOFatal;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 *
 * @author Andrew
 */
public class TrackTransfer {

    private final static Logger LOG = Logger.getLogger("TrackTransfer");
    private final CmdDropDatabase ddb;
    private final CmdCreateTransfer ct;
    private final CmdNewDelivery nd;
    private final CmdAnnotate a;
    private final CmdReport cr;

    /**
     * Report on version...
     *
     * <pre>
     * 20230127 0.1  Split Item and Instance
     * 20230206 0.2  Basic funtionality working
     * 20230215 0.3  Added AnnotateFromFile command
     * 20230320 0.4  Rejigged reporting to generate TSV & CSV outputs
     * 20230404 0.5  Added keywords, and combined annotate and annotateFromFile
     * 20230404 0.6  Added an API
     * </pre>
     */
    static String version() {
        return ("0.5");
    }

    static String copyright = "Copyright 2023 Public Record Office Victoria";

    /**
     * Initialise the analysis regime using command line arguments. Note that in
     * this mode *all* of the VEOs to be checked are passed in as command line
     * arguments.
     *
     * @param args the command line arguments
     * @throws AppFatal if something goes wrong
     */
    public TrackTransfer(String args[]) throws AppFatal {

        // set up the console handler for log messages and set it to output anything
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n");
        Handler[] hs = LOG.getHandlers();
        for (Handler h : hs) {
            h.setLevel(Level.FINEST);
            h.setFormatter(new SimpleFormatter());
        }
        LOG.setLevel(Level.INFO);

        ddb = new CmdDropDatabase();
        ct = new CmdCreateTransfer();
        nd = new CmdNewDelivery();
        a = new CmdAnnotate();
        cr = new CmdReport();
    }

    /**
     * Do a track transfer command. The arguments are the command line arguments
     * in string array (don't include the initial 'tt').
     *
     * @param args the command line argument
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     */
    public void doCommand(String args[]) throws AppFatal, AppError {
        String usage = "'newTransfer', 'newDelivery', 'annotate', 'input', 'fromFile', 'report', dropDatabase', 'printTables', or 'help'";

        // say what we are doing
        LOG.info("******************************************************************************");
        LOG.info("*                                                                            *");
        LOG.info("*                        T R A C K   T R A N S F E R                         *");
        LOG.info("*                                                                            *");
        LOG.info("*                                Version " + version() + "                                 *");
        LOG.info("*               " + copyright + "                 *");
        LOG.info("*                                                                            *");
        LOG.info("******************************************************************************");
        LOG.info("");
        LOG.log(Level.INFO, "Run at {0}", Report.getDateTime());
        LOG.info("");

        // sanity check...
        if (args.length < 1) {
            // LOG.log(Level.WARNING, "Unrecognised subcommand. Expected to see {0}", new Object[]{usage});
            return;
        }
        try {
            switch (args[0].toLowerCase().trim()) {
                case "help":
                    LOG.info("Track transfer sub commands:");
                    LOG.info(" newTransfer: create a new transfer from scratch (can only be done at the start)");
                    LOG.info(" newDelivery: register a new delivery of records from the agency");
                    LOG.info(" annotate: add an annotation to a collection of records");
                    LOG.info(" report: produce a report about the records and events");
                    LOG.info(" dropDatabase: delete a database");
                    LOG.info(" input: a script containing multiple commands (mostly for testing)");
                    LOG.info(" printTables: print the contents of each table (mostly for testing)");
                    LOG.info(" help: print this listing");
                    LOG.info("");
                    break;
                case "dropdatabase":
                    ddb.dropDatabase(args);
                    break;
                case "newtransfer":
                    ct.createTransfer(args);
                    break;
                case "adddelivery":
                case "newdelivery":
                    nd.newDelivery(args);
                    break;
                case "annotate":
                    a.annotateItems(args);
                    break;
                case "report":
                    cr.generateReport(args);
                    break;
                case "input":
                    processFile(args);
                    break;
                case "printtables":
                    printTables(args);
                    break;
                case "//":
                case "!":
                case "":
                case " ":
                    break;
                default:
                    LOG.log(Level.WARNING, "Unrecognised subcommand. Saw ''{0}'' ({1})", new Object[]{args[0], usage});
            }
        } catch (SQLException se) {
            handleSQLException(se);
        }
    }

    /**
     * Drop the database deleting any data in it. The database has
     * to be explicitly specified to prevent accidents.
     *
     * @param database the string representing the database to connect to
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     */
    public void dropDatabase(String database) throws AppFatal, AppError {
        try {
            ddb.dropDatabase(database);
        } catch (SQLException se) {
            handleSQLException(se);
        }
    }

    /**
     * Create a new transfer. This method is called once to create
     * the databases and start a transfer. The description describes the
     * transfer.
     *
     * @param database the string representing the database to create (cannot be
     * null)
     * @param description a description of this transfer (e.g. an ID)
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     * @throws SQLException SQL problem occurred
     */
    public void createTransfer(String database, String description) throws AppFatal, AppError, SQLException {
        try {
            ct.createTransfer(database, description);
        } catch (SQLException se) {
            handleSQLException(se);
        }
    }

    /**
     * Add a new delivery. This may be called multiple times to receive
     * multiple deliveries within the transfer. Description and rootDir are mandatory.
     * Database is optional; if null the database in the current working
     * directory is used.
     *
     * @param database the string representing the database
     * @param description a description of this delivery (e.g. an ID)
     * @param rootDir the root of the tree of items in the delivery
     * @param veoOnly true if only files ending in .veo or .veo.zip are to be processed
     * @param supersedePrevious true if any duplicates will supersede any previous instances
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     */
    public void newDelivery(String database, String description, Path rootDir, boolean veoOnly, boolean supersedePrevious) throws AppFatal, AppError {
        try {
            nd.newDelivery(database, description, rootDir, veoOnly, supersedePrevious);
        } catch (SQLException se) {
            handleSQLException(se);
        }
    }
    
    /**
     * Annotate Items selected by being in a directory (or the tree under the
     * directory). The database is optional, if null the '.mv.db' directory in
     * the current working directory is used. The rootDir must be present and
     * a directory. Any of the description, keywordsToAdd, and keywordsToRemove
     * can be null, but not all of them.
     *
     * @param database the string representing the database
     * @param rootDir the root of the tree of items to be annotated
     * @param description a description of this delivery (e.g. an ID)
     * @param keywordsToAdd list of keywords to add to selected Items
     * @param keywordsToRemove list of keywords to removed from selected Items
     * @param veoOnly true if only files ending in .veo or .veo.zip are to be processed
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     */
    public void annotateItemsByDirectory(String database, Path rootDir, String description, List<String> keywordsToAdd, List<String> keywordsToRemove, boolean veoOnly) throws AppFatal, AppError {
        try {
            a.annotateItemsByDirectory(database, rootDir, description, keywordsToAdd, keywordsToRemove, veoOnly);
        } catch (SQLException se) {
            handleSQLException(se);
        }
    }
    
    
    /**
     * Annotate Items listed in a CSV or TSV file. The database is optional, if
     * null the '.mv.db' directory in the current working directory is used.
     * 
     * The file must not be null, and must be an ordinary file. If forceCSV or
     * forceTSV is set, the input file is assumed to be in that format. If
     * neither is set, the type of input file is assumed from the file
     * extension. If skip is not zero, the first skip lines of the file are
     * ignored (header lines). The name of the Item to be annotated is taken
     * from fileColumn (the first column is zero).
     * 
     * Pattern may be null. If present, it selects rows in the input file
     * identifying Items that will be annotated. A pattern is a sequence of
     * tests separated by commas. Each test is of the form {col}'='{regex>},
     * where {col} is a column number (first column is zero), and {regex} is
     * a Java regular expression testing the value of the column. The line is
     * selected if all tests return true.
     * 
     * Any of the description, keywordsToAdd, and keywordsToRemove can be null,
     * but not all of them.
     *
     * @param database the string representing the database
     * @param file the TSV or CSV file containing the item names to be annotated
     * @param forceCSV true if the file is to be forced to be a CSV file
     * @param forceTSV true if the file is to be forced to be a TSV file
     * @param skip the number of header lines at the start of the file to skip
     * @param fileColumn the column in the file that contains the Item name
     * @param patterns a string containing the tests to select rows of the file
     * @param description a description of this delivery (e.g. an ID)
     * @param keywordsToAdd list of keywords to add to selected Items
     * @param keywordsToRemove list of keywords to removed from selected Items
     * @param veoOnly if true Items names ending in .veo or .veo.zip are to be processed
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     */
    public void annotateItemsByFile(String database, Path file, boolean forceCSV, boolean forceTSV, int skip, int fileColumn, String patterns, String description, List<String> keywordsToAdd, List<String> keywordsToRemove, boolean veoOnly) throws AppFatal, AppError {
        try {
            a.annotateItemsByFile(database, file, forceCSV, forceTSV, skip, fileColumn, patterns, description, keywordsToAdd, keywordsToRemove, veoOnly);
        } catch (SQLException se) {
            handleSQLException(se);
        }
    }

    /**
     * Generate a complete report of all Items, Instances & Events.
     * 
     * @param database the string representing the database (may be null)
     * @param outputFile the file to place the report
     * @throws AppFatal
     * @throws AppError 
     */
    public void completeReport(String database, Path outputFile) throws AppFatal, AppError {
        try {
            cr.generateReport(database, CmdReport.ReportType.COMPLETE, null, outputFile);
        } catch (SQLException se) {
            handleSQLException(se);
        }
    }
    
    /**
     * Generate a complete report of all Items with specified keywords set
     * 
     * @param database the string representing the database (may be null)
     * @param keywords the list of keywords to select Items
     * @param outputFile the file to place the report
     * @throws AppFatal
     * @throws AppError 
     */
    public void keywordReport(String database, ArrayList<String> keywords, Path outputFile) throws AppFatal, AppError {
        try {
            cr.generateReport(database, CmdReport.ReportType.KEYWORD, keywords, outputFile);
        } catch (SQLException se) {
            handleSQLException(se);
        }
    }

    /**
     * Read a control file containing Track Transfer commands, one per line. The
     * tokens are split on spaces, except spaces inside pairs of double quotes.
     *
     * @param args command line arguments
     * @throws AppFatal
     * @throws SQLException
     */
    private void processFile(String args[]) throws AppFatal, SQLException {
        Path p;
        FileInputStream fis;     // source of control file to build VEOs
        InputStreamReader isr;
        BufferedReader br;
        String line;
        String[] tokens;
        int i;

        // final argument is the file name
        if (args.length < 2 || args[1] == null) {
            throw new AppFatal("Input command: missing file name. Usage: trackTransfer input <fileName>");
        }
        p = checkPath("command file", args[1], false);

        // read control file
        try {
            fis = new FileInputStream(p.toFile().toString());
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);

            while ((line = br.readLine()) != null) {
                tokens = line.split("\\s(?=(([^\"]*\"){2})*[^\"]*$)\\s*");
                for (i = 0; i < tokens.length; i++) {
                    if (tokens[i].startsWith("\"")) {
                        tokens[i] = tokens[i].substring(1);
                    }
                    if (tokens[i].endsWith("\"")) {
                        tokens[i] = tokens[i].substring(0, tokens[i].length() - 1);
                    }
                }
                try {
                    doCommand(tokens);
                } catch (AppError ae) {
                    LOG.log(Level.WARNING, "****** Something went wrong: {0}", new Object[]{ae.getMessage()});
                    break;
                }
            }
            br.close();
            isr.close();
            fis.close();
        } catch (FileNotFoundException e) {
            throw new AppFatal("Failed to open control file '" + args[1] + "'" + e.toString());
        } catch (IOException e) {
            throw new AppFatal("Failed reading the control file '" + args[1] + "'" + e.toString());
        }
    }

    /**
     * Print out the contents of the tables. Mainly for debugging.
     *
     * @param args
     * @throws AppFatal
     * @throws AppError
     * @throws SQLException
     */
    private void printTables(String args[]) throws AppFatal, AppError, SQLException {
        // final argument is the database name
        if (args.length < 2 || args[1] == null) {
            throw new AppError("Print tables command: missing database name. Usage: trackTransfer printtables <databaseURI>");
        }
        SQLTable.connect(args[1]);
        // LOG.info(TblTransfer.printTable());
        LOG.info(TblItem.printTable());
        //LOG.info(TblDelivery.printTable());
        //LOG.info(TblInstance.printTable());
        //LOG.info(TblEvent.printTable());
        //LOG.info(TblInstanceEvent.printTable());
        LOG.info(TblItemKeyword.printTable());
        LOG.info(TblKeyword.printTable());
        SQLTable.disconnect();
    }

    /**
     * Check a file to see that it exists and is of the correct type (regular
     * file or directory). The program terminates if an error is encountered.
     *
     * @param type a String describing the file to be opened
     * @param name the file name to be opened
     * @param isDirectory true if the file is supposed to be a directory
     * @throws VEOFatal if the file does not exist, or is of the correct type
     * @return the File opened
     */
    private Path checkPath(String type, String name, boolean isDirectory) throws AppFatal {
        Path p;
        String safe;

        safe = name.replaceAll("\\\\", "/");
        try {
            p = Paths.get(safe);
        } catch (InvalidPathException ipe) {
            throw new AppFatal(type + " '" + safe + "' is not a valid file name." + ipe.getMessage());
        }

        if (!Files.exists(p)) {
            throw new AppFatal(type + " '" + p.toAbsolutePath().toString() + "' does not exist");
        }
        if (isDirectory && !Files.isDirectory(p)) {
            throw new AppFatal(type + " '" + p.toAbsolutePath().toString() + "' is a file not a directory");
        }
        if (!isDirectory && Files.isDirectory(p)) {
            throw new AppFatal(type + " '" + p.toAbsolutePath().toString() + "' is a directory not a file");
        }
        return p;
    }

    /**
     * Handle an SQL exception. It does this by converting the SQL Exception
     * into an AppFatal (it's our error, users should never be able to force an
     * SQL exception) and then throwing it. The only exception is if we are
     * ignoring the SQL exception, in which case this simply returns.
     *
     * @param se the SQL exception
     * @throws AppFatal
     */
    private void handleSQLException(SQLException se) throws AppFatal {
        StringBuilder sb = new StringBuilder();

        for (Throwable t : se) {
            if (t instanceof SQLException) {
                SQLException e = (SQLException) t;
                String sqlState = e.getSQLState();
                if (sqlState != null) {
                    // X0Y32: Jar file already exists in schema
                    if (sqlState.equalsIgnoreCase("X0Y32")) {
                        return;
                    }
                    // 42Y55: Table already exists in schema
                    if (sqlState.equalsIgnoreCase("42Y55")) {
                        return;
                    }
                }
                sb.append("SQL Error: SQLState: " + sqlState);
                sb.append(" Error code: " + e.getErrorCode());
                sb.append(" Message: " + e.getMessage());
                // t.printStackTrace(System.err);
            }
        }
        throw new AppFatal(sb.toString());
    }

    /**
     * Main program...
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        TrackTransfer tt;

        try {
            tt = new TrackTransfer(args);
            tt.doCommand(args);
        } catch (AppError e) {
            System.err.println("Command failed: " + e.getMessage());
        } catch (AppFatal e) {
            System.err.println("Fatal error (should not have occurred): " + e.toString());
            e.printStackTrace();
        }
    }

}
