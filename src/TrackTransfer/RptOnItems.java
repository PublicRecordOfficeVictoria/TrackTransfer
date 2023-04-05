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
import java.util.ArrayList;
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

    public void generate(Path output, String header, ArrayList<String> keywords, String sortby) throws SQLException, IOException, AppError {
        ResultSet items;
        int i;
        StringBuilder where = new StringBuilder();

        open(output);

        // write the header
        writeHeader(header, keywords);

        // get the items with the specified keywords
        for (i = 0; i < keywords.size(); i++) {
            if (keywords.get(i).equalsIgnoreCase("Custody-accepted")) {
                where.append("ITEM.STATE='C'");
            } else if (keywords.get(i).equalsIgnoreCase("Abandoned")) {
                where.append("ITEM.STATE='A'");
            } else if (keywords.get(i).equalsIgnoreCase("Incomplete")) {
                where.append("ITEM.STATE='P'");
            } else {
                where.append("KEYWORD.KEYWORD='" + keywords.get(i) + "'");
            }
            if (i < keywords.size() - 1) {
                where.append(" OR ");
            }
        }
        i = 0;
        items = SQLTable.query("ITEM left join ITEM_KEYWORD on ITEM.ITEM_ID=ITEM_KEYWORD.ITEM_ID left join KEYWORD on ITEM_KEYWORD.KEYWORD_ID=KEYWORD.KEYWORD_ID", "*", where.toString(), sortby);
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
    private void writeHeader(String header, ArrayList<String> keywords) throws IOException, SQLException {
        int i;
        switch (format) {
            case TEXT:
                w.append("Report on Items ");
                w.append(header);
                w.append(": ");
                for (i = 0; i < keywords.size(); i++) {
                    w.append("'");
                    w.append(keywords.get(i));
                    w.append("'");
                    if (keywords.size() > 1) {
                        if (i < keywords.size() - 2) {
                            w.append(", ");
                        } else if (i < keywords.size()-1) {
                            w.append(", or ");
                        }
                    }
                }
                w.append("\n");
                w.append("Run at " + getDateTime() + "\n");
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
