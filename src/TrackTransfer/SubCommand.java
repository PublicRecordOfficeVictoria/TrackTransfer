/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import VERSCommon.AppFatal;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * This class has the common routines for a sub command within Track TblTransfer
 *
 * @author Andrew
 */
public class SubCommand {

    protected boolean help;     // true if printing out the help text
    protected String database;  // URI to connect to database
    protected static TblTransfer transferTbl;       // SQL table for transfers
    protected static TblDelivery deliveryTbl;       // SQL table for deliveries

    protected SubCommand() {
        transferTbl = null;
        deliveryTbl = null;
    }

    protected void connectDB(String database) throws SQLException, AppFatal {
        SQL.connect(database);

        // create the tables
        transferTbl = new TblTransfer();
        deliveryTbl = new TblDelivery();
    }
    
    protected void disconnectDB() throws SQLException {
        SQL.disconnect();
    }
}
