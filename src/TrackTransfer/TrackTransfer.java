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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
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

    /**
     * Report on version...
     *
     * <pre>
     * 20230127   0.1 Split Item and Instance
     * 20230206   0.2 Basic funtionality working
     * 20230215   0.3 Added AnnotateFromFile command
     * </pre>
     */
    static String version() {
        return ("0.3");
    }

    static String copyright = "Copyright 2022 Public Record Office Victoria";

    /**
     * Initialise the analysis regime using command line arguments. Note that in
     * this mode *all* of the VEOs to be checked are passed in as command line
     * arguments.
     *
     * @param args the command line arguments
     * @throws AppFatal if something goes wrong
     */
    public TrackTransfer(String args[]) throws AppFatal {
        SimpleDateFormat sdf;
        TimeZone tz;

        // set up the console handler for log messages and set it to output anything
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n");
        Handler[] hs = LOG.getHandlers();
        for (Handler h : hs) {
            h.setLevel(Level.FINEST);
            h.setFormatter(new SimpleFormatter());
        }
        LOG.setLevel(Level.INFO);

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
        tz = TimeZone.getTimeZone("GMT+10:00");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss+10:00");
        sdf.setTimeZone(tz);
        LOG.log(Level.INFO, "Run at {0}", sdf.format(new Date()));
        LOG.info("");
    }

    private void doCommand(String args[]) throws AppFatal, AppError, SQLException {
        String usage = "'newTransfer', 'newDelivery', 'annotate', 'input', 'fromFile', 'report', dropDatabase', 'printTables', or 'help'";

        // sanity check...
        if (args.length < 1) {
            // LOG.log(Level.WARNING, "Unrecognised subcommand. Expected to see {0}", new Object[]{usage});
            return;
        }

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
                CmdDropDatabase db = new CmdDropDatabase();
                db.doIt(args);
                break;
            case "newtransfer":
                CmdCreateTransfer ct = new CmdCreateTransfer();
                ct.doIt(args);
                break;
            case "adddelivery":
            case "newdelivery":
                CmdNewDelivery nd = new CmdNewDelivery();
                nd.doIt(args);
                break;
            case "annotate":
                CmdAnnotate a = new CmdAnnotate();
                a.doIt(args);
                break;
            case "annotatefromfile":
                CmdAnnotateFromFile ff = new CmdAnnotateFromFile();
                ff.doIt(args);
                break;
            case "report":
                CmdReport cr = new CmdReport();
                cr.doIt(args);
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
            throw new AppFatal("Print tables command: missing database name. Usage: trackTransfer printtables <databaseURI>");
        }
        SQL.connect(args[1]);
        LOG.info(TblTransfer.printTable());
        LOG.info(TblItem.printTable());
        LOG.info(TblDelivery.printTable());
        LOG.info(TblInstance.printTable());
        LOG.info(TblEvent.printTable());
        LOG.info(TblInstanceEvent.printTable());
        SQL.disconnect();
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
        } catch (SQLException se) {
            System.err.println("Fatal error (should not have occurred): " + se.toString());
            for (Throwable t : se) {
                if (t instanceof SQLException) {
                    SQLException e = (SQLException) t;
                    String sqlState = e.getSQLState();
                    if (sqlState != null) {
                        // X0Y32: Jar file already exists in schema
                        if (sqlState.equalsIgnoreCase("X0Y32")) {
                            break;
                        }
                        // 42Y55: Table already exists in schema
                        if (sqlState.equalsIgnoreCase("42Y55")) {
                            break;
                        }
                    }
                    System.err.print("SQLState: " + sqlState);
                    System.err.println(" Error code: " + e.getErrorCode());
                    System.err.println("Message: " + e.getMessage());
                    t.printStackTrace(System.err);
                }
            }
        }
    }

}
