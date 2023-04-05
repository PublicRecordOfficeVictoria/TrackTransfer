/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TrackTransfer;

import VERSCommon.AppError;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;

/**
 *
 * @author Andrew
 */
public abstract class Report {
    private final static Logger LOG = Logger.getLogger("TrackTransfer.Report");
    private FileOutputStream fos;
    private OutputStreamWriter osw;
    protected BufferedWriter w;
    protected ReportType format; // type of report to generate (based on file name)

    /**
     * Types of reports that can be generated
     */
    public enum ReportType {
        TEXT,       // report in plain text (default)
        CSV,        // report as CSV file
        TSV,        // report as TSV file
        JSON        // report as JSON file
    }

    /**
     * Open the Writer for output of the report. The format of the required
     * report is decided upon based on the file extension of the output file.
     *
     * @param output file name of the report
     * @throws IOException
     */
    protected void open(Path output) throws IOException, AppError {
        String filename;
        
        filename = output.getFileName().toString().toLowerCase();
        System.out.println("File: '"+filename+"'");
        if (filename.endsWith(".txt")) {
            format = ReportType.TEXT;
        } else if (filename.endsWith(".csv")) {
            format = ReportType.CSV;
        } else if (filename.endsWith(".tsv")) {
            format = ReportType.TSV;
        } else {
            throw new AppError("Report file name does not end with '.txt', '.tsv', or '.csv'");
        }
        System.out.println("Format: "+format);
        
        fos = new FileOutputStream(output.toFile());
        osw = new OutputStreamWriter(fos, "UTF-8");
        w = new BufferedWriter(osw);
    }

    /**
     * Close the Writer at the completion of the report. Ignore any IOExceptions
     */
    protected void close() {
        try {
            w.close();
            osw.close();
            fos.close();
        } catch (IOException ioe) {
        }
    }
    
    /**
     * Get the current data and time in the local time zone.
     * 
     * @return 
     */
    public static String getDateTime() {
        ZoneId zi;
        ZonedDateTime zdt;
        
        zi = ZoneId.systemDefault();
        zdt = ZonedDateTime.now(zi);
        return zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"));
    }

    /**
     * Encode an array of values according to the specific encoding rules for
     * the format
     *
     * @param args array of values
     * @throws java.io.IOException
     */
    protected void encode(String[] args) throws IOException {
        int i;

        for (i = 0; i < args.length; i++) {
            if (format == ReportType.CSV) {
                encode2CSV(args[i]);
            } else {
                encode2TSV(args[i]);
            }
            if (i < args.length - 1) {
                tsvCSVSeparator();
            }
        }
    }

    /**
     * Encode a string value to sit inside a CSV file (see RFC4180). If the
     * value contains a comma, double quote, or end of line, it is enclosed in
     * double quotes. Any double quotes inside the value are escaped by being
     * doubled.
     *
     * @param value
     */
    private void encode2CSV(String value) throws IOException {
        if (value.contains(",") || value.contains("\"") || value.contains("\r\n")) {
            w.write('"');
            w.write(value.replace("\"", "\"\""));
            w.write('"');
        } else {
            w.write(value);
        }
    }

    /**
     * Encode a string value to sit inside a TSV file. If the value contains a
     * '\', '\t', '\n', or a '\r' character, it is replaced by the string "\\",
     * "\t", "\n", or "\r" respectively.
     *
     * @param value
     */
    private void encode2TSV(String value) throws IOException {
        String s;

        s = value.replace("\\", "\\\\");
        s = s.replace("\t", "\\t");
        s = s.replace("\n", "\\n");
        s = s.replace("\r", "\\r");
        w.write(s);
    }

    /**
     * Return the correct tsvCSVSeparator for a TSV or CSV file
     */
    protected void tsvCSVSeparator() throws IOException {
        if (format == ReportType.CSV) {
            w.write(",");
        } else {
            w.write("\t");
        }
    }

    /**
     * Return the correct EOL for a TSV or CSV file
     * @throws java.io.IOException
     */
    protected void writeEOL() throws IOException {
        switch (format) {
            case CSV:
                w.write("\r\n");
                break;
            default:
                w.write("\n");
                break;
        }
    }
}
