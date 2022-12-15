package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
 * This command generates a report on the items in a transfer
 *
 * @author Andrew
 */
public class CmdReport extends SubCommand {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdAnnotate");
    private Path outputFile;       // report file

    public CmdReport() throws AppFatal {
        super();
    }

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        FileOutputStream fos;
        OutputStreamWriter osw;
        BufferedWriter bw;
        int itemKey;
        ResultSet items, events;
        
        database = "jdbc:h2:./trackTransfer";
        
        config(args);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'Annotate' command line arguments:");
            LOG.info(" Mandatory:");
            LOG.info("  -o <filename>: output file for the report");
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
        if (outputFile == null) {
            throw new AppFatal("Output file is not specified (-o)");
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Generate report");
        LOG.log(Level.INFO, " Database: {0}", database);
        LOG.log(Level.INFO, " Report: {0}", outputFile.toString());
        if (LOG.getLevel() == Level.INFO) {
            LOG.info(" Logging: verbose");
        } else if (LOG.getLevel() == Level.FINE) {
            LOG.info(" Logging: debug");
        } else {
            LOG.info(" Logging: warnings & errors only");
        }

        // open the output file for writing
        try {
            fos = new FileOutputStream(outputFile.toFile());
            osw = new OutputStreamWriter(fos, "UTF-8");
            bw = new BufferedWriter(osw);
            bw.append("Report of all items\n\n");

            // connect to the database and create the tables
            connectDB(database);

            // go through the items
            items = TblItem.query("*", null);
            while (items.next()) {
                bw.append("Item: '");
                bw.append(TblItem.getFilename(items));
                bw.append(" (");
                bw.append(TblItem.getStatus(items));
                bw.append(")");
                bw.append("'\n");
                bw.append(" Last seen at '");
                bw.append(TblItem.getFilepath(items));
                bw.append("'\n");
                itemKey = TblItem.getItemId(items);
                
                // get events related to this item
                bw.append(" Events:\n");
                events = SQL.query("ITEM_EVENT, EVENT", "*", "ITEM_EVENT.EVENT_ID=EVENT.EVENT_ID AND ITEM_EVENT.ITEM_ID="+itemKey);
                // events = TblEvent.query("*", "ITEM_ID="+itemKey);
                while (events.next()) {
                    bw.append("  ");
                    bw.append(TblEvent.getWhenReceived(events));
                    bw.append(" ");
                    bw.append(TblEvent.getDescription(events));
                    bw.append("\n");
                }
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
        LOG.log(Level.INFO, "Report generated from ({0}) to ''{1}''", new Object[]{database,outputFile.toString()});
    }

    public void config(String args[]) throws AppFatal {
        String usage = "-db <databaseURL> -o <file>";
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
                    // output file
                    case "-o":
                        i++;
                        outputFile = Paths.get(args[i]);
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

    private void annotateItems(Path dir, int eventKey) throws AppFatal, SQLException {
        int itemKey;
        String s1;

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path entry : ds) {
                if (entry.toFile().isDirectory()) {
                    annotateItems(entry, eventKey);
                } else {
                    s1 = entry.getFileName().toString().replace("'", "");
                    if ((itemKey = TblItem.itemExists(s1)) != 0) {
                        TblItemEvent.add(itemKey, eventKey);
                    }
                }
            }
        } catch (DirectoryIteratorException | IOException e) {
            throw new AppFatal(e.getMessage());
        }
    }
}
