/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import VERSCommon.AppError;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Generate a custody accepted report. This lists all items that have the status
 * 'custody-accepted'.
 *
 * @author Andrew
 */
public class RptOnItems extends Report {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.RptOnItems");

    public RptOnItems() {
    }

    /**
     * Generate a complete report of all items for which custody has been
     * accepted
     *
     * @param output file name of the report
     * @param header
     * @param where
     * @param what
     * @throws SQLException
     * @throws IOException
     * @throws VERSCommon.AppError
     */
    public void generate(Path output, String header, String where, String sortby) throws SQLException, IOException, AppError {
        ResultSet items;
        int i;

        open(output);

        // write the header
        writeHeader(header);

        // go through the items
        i = 0;
        items = TblItem.query("*", where, sortby);
        while (items.next()) {

            // write a heartbeat on stdout to show how far we've come
            i++;
            if (i % 100 == 0) {
                System.out.println(i);
            }

            // write current item (if separating out items)
            writeItem(items);
        }
        
        close();
    }

    /**
     * Write the header depending on the format
     *
     * @param header the title of the output
     * @throws IOException
     * @throws SQLException
     */
    private void writeHeader(String header) throws IOException, SQLException {
        switch (format) {
            case TEXT:
                w.append(header);
                w.append("\n");
                w.append("Run at "+getDateTime()+"\n");
                w.append("\n");
                break;
            case CSV:
            case TSV:
                encode(TblItem.tableOut(null));
                writeEOL();
                break;
            default:
                break;
        }
    }

    /**
     * Write an Item
     *
     * @param item
     * @throws IOException
     * @throws SQLException
     */
    private void writeItem(ResultSet item) throws IOException, SQLException {
        switch (format) {
            case TEXT:
                w.append(TblItem.reportItem(item));
                w.append("\n");
                break;
            case CSV:
            case TSV:
                encode(TblItem.tableOut(item));
                writeEOL();
                break;
            default:
                break;
        }
    }
}
