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
import org.json.simple.JSONObject;

/**
 * This generates a
 *
 * @author Andrew
 */
public class RptComplete extends Report {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.RptComplete");

    public RptComplete() {
    }

    /**
     * Generate a complete report of all items received, all instances for each
     * item, and all events for each instance.
     *
     * @param output file name of the report
     * @throws SQLException
     * @throws IOException
     * @throws VERSCommon.AppError
     */
    public void generate(Path output) throws SQLException, IOException, AppError {
        int itemKey, instanceKey;
        ResultSet items, instances, events;
        int i;

        open(output);

        // write the header
        writeHeader();

        // go through the items
        i = 0;
        items = TblItem.query("*", null, "FILENAME");
        while (items.next()) {

            // write a heartbeat on stdout to show how far we've come
            i++;
            if (i % 100 == 0) {
                System.out.println(i);
            }

            // write current item (if separating out items)
            writeItem(items);

            // get instances of this item
            itemKey = TblItem.getItemId(items);
            instances = TblInstance.query("*", "ITEM_ID=" + itemKey, "ITEM_ID");
            // instances = SQLTable.query("select * from INSTANCE where ITEM_ID="+itemKey+" ORDER BY ITEM_ID");
            while (instances.next()) {
                writeInstance(instances);

                // get events related to this instance
                instanceKey = TblInstance.getInstanceId(instances);
                events = SQLTable.query("INSTANCE_EVENT join EVENT on INSTANCE_EVENT.EVENT_ID=EVENT.EVENT_ID", "*", "INSTANCE_EVENT.INSTANCE_ID=" + instanceKey, "EVENT_ID");
                while (events.next()) {
                    writeEvent(items, instances, events);
                }
            }
        }
        
        close();
    }

    /**
     * Write the header depending on the format
     *
     * @throws IOException
     * @throws SQLException
     */
    private void writeHeader() throws IOException, SQLException {
        switch (format) {
            case TEXT:
                w.append("COMPLETE REPORT (all Items/Instances/Events)\n");
                w.append("Run \n");
                w.append("\n");
                break;
            case CSV:
            case TSV:
                encode(TblItem.tableOut(null));
                tsvCSVSeparator();
                encode(TblInstance.tableOut(null));
                tsvCSVSeparator();
                encode(TblEvent.tableOut(null));
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
            default:
                break;
        }
    }

    /**
     * Write an Instance
     *
     * @param item
     * @throws IOException
     * @throws SQLException
     */
    private void writeInstance(ResultSet instance) throws IOException, SQLException {
        switch (format) {
            case TEXT:
                w.append("  ");
                w.append(TblInstance.reportInstance(instance));
                writeEOL();
                break;
            case CSV:
            case TSV:
            default:
                break;
        }
    }

    /**
     * Write an Instance
     *
     * @param item
     * @throws IOException
     * @throws SQLException
     */
    private void writeEvent(ResultSet item, ResultSet instance, ResultSet event) throws IOException, SQLException {
        switch (format) {
            case TEXT:
                w.append("    ");
                w.append(TblEvent.reportEvent(event));
                writeEOL();
                break;
            case CSV:
            case TSV:
                encode(TblItem.tableOut(item));
                tsvCSVSeparator();
                encode(TblInstance.tableOut(instance));
                tsvCSVSeparator();
                encode(TblEvent.tableOut(event));
                writeEOL();
                break;
            default:
                break;
        }
    }

}
