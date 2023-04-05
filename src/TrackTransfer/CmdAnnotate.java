package TrackTransfer;

import VERSCommon.AppError;
import VERSCommon.AppFatal;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
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
public class CmdAnnotate extends Command {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdAnnotate");
    private String desc;        // description specified by the user
    private String desc1;       // description of the event assuming all the state changes are valid
    private int eventKey1;      // event matching desc1 (this will only be populated when an item that needs it is encountered)
    private String desc2;       // description of the event if couldn't change from Custody-accepted to Abandoned
    private int eventKey2;      // event matching desc2 (this will only be populated when an item that needs it is encountered)
    private final List<Keyword> keywords; // keywords to add to the selected items
    private String stateChange; // stateChange of items 'X'=unchanged, 'P'=to processing, 'A'=to abandoned, 'C'=to custody accepted
    private Path rootDir;       // root directory containing the objects being annotated (if items identified by a directory tree walk)
    private Path inputFile;     // inputFile to process for Item names & condition (if items identified by a inputFile)
    private int skip;           // number of lines in the header to skip
    private List<MatchPattern> patterns; // patterns to match agains C/TSV files
    private int fileColumn;     // column in which to find the filename
    private boolean csv;        // true if files are separated by commas rather than tabs
    private int count;          // number of items annotated
    private String usage = "[-db <database>] [-desc <text>] [-set <keyword>] [-remove <keyword>] [-custody-accepted] [-abandoned] [-dir <directory>] [[-in] file [-skip <count>] [-csv] [-tsv] [-pattern <pattern>] [itemcol <column>]] [-v] [-d] [-help]";

    public CmdAnnotate() throws AppFatal {
        super();
        keywords = new ArrayList<>();
    }

    /**
     * Private class that holds a keyword that is being added or removed from
     * the specified items. Two specific keywords ('Custody-accepted' and
     * 'Abandoned' are held indirectly in Items as the state of the item, and
     * this is indicated by 'silent'. The class also holds the key of the
     * keyword in the Keyword table (or 0 if it needs to be added).
     */
    private class Keyword {

        String keyword;     // keyword
        boolean add;        // true if adding keyword, false if removing
        boolean silent;     // true if adding/removing keyword will be handled by a state change)
        int key;            // index in Keyword table

        public Keyword(String keyword, boolean add, boolean silent) {
            this.keyword = keyword;
            this.add = add;
            this.silent = silent;
            key = 0;
        }
    }

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

    public void doIt(String args[]) throws AppFatal, AppError, SQLException {
        StringBuilder sb = new StringBuilder();
        String s;

        count = 0;
        keywords.clear();
        stateChange = "X";
        eventKey1 = 0;
        eventKey2 = 0;

        // getting items from a CSV or TSV inputFile
        skip = 0;
        fileColumn = -1;
        csv = false;
        patterns = null;

        // getting items from a inputFile names in a directory
        // what does the user want the command to do
        config(args, usage);

        // just asked for help?
        if (help) {
            LOG.setLevel(Level.INFO);
            LOG.info("'Annotate' command line arguments:");
            LOG.info(" Conditional:");
            LOG.info("  If getting the Items from file names in a directory:");
            LOG.info("   -dir <filename>: name of directory holding objects being annotated");
            LOG.info("  If getting the Items from a TSV or CSV file:");
            LOG.info("   -file <filename>: name of file containing the names of the Items");
            LOG.info("   -itemcol <column>: column containing filename (Item name). First column = 0");
            LOG.info("");
            LOG.info(" Optional:");
            LOG.info("  -set <keyword>: Set a keyword (special values 'abandoned' & 'custody-accepted'");
            LOG.info("  -remove <keyword>: Remove a keyword");
            LOG.info("  -desc <description>: annotation text");
            LOG.info("  -custody-accepted: equivalent to '-set Custody-Accepted'");
            LOG.info("  -abandoned: equivalent to '-set Abandoned'");
            LOG.info("  -db <database>: Database name (default based on .mv.db file in current working directory)");
            LOG.info("");
            LOG.info("  If getting the items from a TSV or CSV file:");
            LOG.info("   -skip <count>: Number of lines at head of file to skip");
            LOG.info("   -pattern <pattern>: columns/patterns to match against. Formant <columnNumber>=<regex>,... Default always select");
            LOG.info("   -tsv: file is a TSV file (default)");
            LOG.info("   -csv: file is a CSV file");
            genericHelp();
            LOG.info("");
            LOG.info("One or both of -set/-remove/-custody-accepted/-abandoned and -desc must be present");
            return;
        }

        // check necessary fields have been specified
        if (desc == null && keywords.isEmpty()) {
            throw new AppError("At least one of '-desc', '-set', or '-remove' must be set ");
        }
        if ((rootDir != null && inputFile != null) || (rootDir == null && inputFile == null)) {
            throw new AppFatal("Must specify either the directory containing the Items being annotated (-dir) OR a TSV/CSV file containing the Item names (-in)");
        }
        // check specific details if getting Items from a CSV/TSV inputFile
        if (inputFile != null) {
            if (fileColumn < 0) {
                throw new AppError("When reading Items from a file, the column in which the filename (Item name) is to be found must be specified (-filename)");
            }
            if (skip < 0) {
                throw new AppError("When reading Items from a file, the skip count must be a positive number (-skip)");
            }
            if (!inputFile.toFile().exists()) {
                throw new AppError("Load File: file '" + inputFile.toString() + "' does not exist");
            }
            if (!inputFile.toFile().isFile()) {
                throw new AppError("Load File: file '" + inputFile.toString() + "' is not a directory");
            }
        }
        // if getting Items from a directory, check if the root directory exists and is a directory
        if (rootDir != null) {
            if (!rootDir.toFile().exists()) {
                throw new AppError("New Delivery: directory '" + rootDir.toString() + "' does not exist");
            }
            if (!rootDir.toFile().isDirectory()) {
                throw new AppError("New Delivery: directory '" + rootDir.toString() + "' is not a directory");
            }
        }

        // say what we are doing
        LOG.info("Requested:");
        LOG.info(" Annotate items");
        LOG.log(Level.INFO, " Database: {0}", database == null ? "Derived from .mv.db filename" : database);
        if (desc != null) {
            LOG.log(Level.INFO, " Description to be added: ''{0}''", desc);
        } else {
            LOG.log(Level.INFO, " No description to be added");
        }
        if ((s = keywordChanges(true)) != null) {
            LOG.log(Level.INFO, " {0}", s);
        } else {
            LOG.info(" No keywords to be added");
        }
        if ((s = keywordChanges(false)) != null) {
            LOG.log(Level.INFO, " {0}", s);
        } else {
            LOG.info(" No keywords to be removed");
        }
        switch (stateChange) {
            case "X":
                LOG.info(" State unchanged. ");
                break;
            case "P":
                LOG.info(" State changed to processing. ");
                break;
            case "A":
                LOG.info(" State changed to abandoned. ");
                break;
            case "C":
                LOG.info(" State changed to custody accepted ");
                break;
            default:
                LOG.info(" Unknown state change. ");
                break;
        }
        if (rootDir != null) {
            LOG.log(Level.INFO, " Getting Item names from file names in a directory");
            LOG.log(Level.INFO, "  Directory of Items being annotated: {0}", rootDir.toString());
        }
        if (inputFile != null) {
            LOG.log(Level.INFO, " Getting Item names from a TSV/CSV file");
            LOG.log(Level.INFO, "  File containing Item names: {0}", inputFile.toString());
            if (csv) {
                LOG.info("  Columns in file are separated by commas (CSV file)");
            } else {
                LOG.info("  Columns in file are separated by tabs (TSV file)");
            }
            LOG.log(Level.INFO, "  Lines to skip at start of file: {0}", skip);
            LOG.log(Level.INFO, "  Patterns to match against: {0}", patterns == null ? "Match all lines" : patterns.toString());
            LOG.log(Level.INFO, "  Item name (filename) column: {0}", fileColumn);
        }
        genericStatus();

        // Append to the specified description details about the keywords added
        // or removed, and whether the items were finalised or unfinalised
        if (desc != null) {
            sb.append(desc);
            sb.append(". ");
        }
        if ((s = keywordChanges(true)) != null) {
            sb.append(s);
        }
        if ((s = keywordChanges(false)) != null) {
            sb.append(s);
        }
        desc2 = sb.toString() + "State unchanged.";
        switch (stateChange) {
            case "X":
                sb.append("State unchanged. ");
                break;
            case "P":
                sb.append("State changed to processing. ");
                break;
            case "A":
                sb.append("State changed to abandoned. ");
                break;
            case "C":
                sb.append("State changed to custody accepted ");
                break;
            default:
                sb.append("Unknown state change. ");
                break;
        }
        desc1 = sb.toString();

        // connect to the database and create the tables
        connectDB();

        // find the keywords in the Keyword table
        findKeywords();

        // Find Item names to process
        if (rootDir != null) {
            annotateItemsByFilename(rootDir);
        }
        if (inputFile != null) {
            annotateItemsByFile(inputFile, patterns);
        }

        // remove any keywords that are no longer referenced
        removeDeadKeywords();

        disconnectDB();

        // acknowledge creation
        LOG.log(Level.INFO, " {0} items annotated in ({1}) with event {2} or {3}", new Object[]{count, database, eventKey1, eventKey2});
    }

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
            // add a keyword to the items
            case "-set":
                i++;
                if (args[i].equalsIgnoreCase("Custody-accepted")) {
                    stateChange = "C";
                    addKeyword(args[i], true, true);
                } else if (args[i].equalsIgnoreCase("Abandoned")) {
                    stateChange = "A";
                    addKeyword(args[i], true, true);
                } else {
                    addKeyword(args[i], true, false);
                }
                i++;
                j = 2;
                break;
            // custody has been accepted of the items
            case "-custody-accepted":
                stateChange = "C";
                addKeyword("Custody-accepted", true, true);
                i++;
                j = 1;
                break;
            // items have been abandoned
            case "-abandoned":
                stateChange = "A";
                addKeyword("Abandoned", true, true);
                i++;
                j = 1;
                break;
            // remove a keyword from the items
            case "-remove":
                i++;
                if (args[i].equalsIgnoreCase("Custody-accepted") || args[i].equalsIgnoreCase("Abandoned")) {
                    stateChange = "P";
                    addKeyword(args[i], false, true);
                } else {
                    addKeyword(args[i], false, false);
                }
                i++;
                j = 2;
                break;
            // directory that contains the items in the delivery
            case "-dir":
                i++;
                rootDir = Paths.get(args[i]);
                i++;
                j = 2;
                break;
            // directory that contains the items in the delivery
            case "-file":
                i++;
                inputFile = Paths.get(args[i]);
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
            // column in which to find the inputFile name
            case "-itemcol":
                i++;
                try {
                    fileColumn = Integer.parseInt(args[i]);
                } catch (NumberFormatException nfe) {
                    throw new AppError("Failed converting item column to an integer: " + nfe.getMessage());
                }
                i++;
                j = 2;
                break;
            // inputFile is a CSV inputFile
            case "-csv":
                i++;
                csv = true;
                j = 1;
                break;
            // inputFile is a TSV inputFile
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
     * Add a keyword to the list of keywords. If the keyword is already in the
     * list, it is not readded, instead whether the keyword is to be added or
     * removed is updated from the newly found instance.
     *
     * @param keyword keyword being added/removed
     * @param add true if keyword is being added, false if being removed
     * @param silent true if keyword is not to be added to Keyword table; will
     * be recorded as a state change in Item
     * @param add
     */
    private void addKeyword(String keyword, boolean add, boolean silent) {
        int i;

        for (i = 0; i < keywords.size(); i++) {
            if (keywords.get(i).keyword.equals(keyword)) {
                keywords.get(i).add = add;
                return;
            }
        }
        keywords.add(new Keyword(keyword, add, silent));
    }

    /**
     * Look up the keywords and remember their key. If the keyword is not in the
     * Keyword table and we are deleting the keyword, remove the keyword from
     * the list of keyword changes (it can't be removed as it doesn't exist). If
     * the keyword is being added and it doesn't already exist in the Keyword
     * table, DON'T add the keyword yet. We wait until we actually find an Item
     * to add the keyword. Ignore "Custody-accepted" and "Abandoned" as they are
     * never in the Keywords table and are dealt with separately.
     *
     * @throws SQLException
     */
    private void findKeywords() throws SQLException {
        int i, key;
        Keyword keyword;
        ResultSet rsKeyword;

        for (i = 0; i < keywords.size(); i++) {
            keyword = keywords.get(i);
            if (keyword.keyword.equalsIgnoreCase("custody-accepted") || keyword.keyword.equalsIgnoreCase("abandoned")) {
                continue;
            }
            rsKeyword = TblKeyword.query("KEYWORD_ID", "KEYWORD='" + keyword.keyword + "'", null);
            if (rsKeyword.next()) {
                key = TblKeyword.getKeywordId(rsKeyword);
                keyword.key = key;
            } else if (!keyword.add) {
                keywords.remove(i);
            }
        }
    }

    /**
     * Remove keywords that are no longer referenced by any items (i.e. we
     * removed keywords from items, and this removed the last reference to a
     * keyword).
     *
     * @throws SQLException
     */
    private void removeDeadKeywords() throws SQLException {
        int i;
        Keyword keyword;
        ResultSet rs;

        // go through list of keywords to change, and look at the removal ones
        // check if there are any links in the Item/Keyword table. If there are
        // none, delete the keyword
        for (i = 0; i < keywords.size(); i++) {
            keyword = keywords.get(i);
            if (keyword.keyword.equalsIgnoreCase("custody-accepted") || keyword.keyword.equalsIgnoreCase("abandoned")) {
                continue;
            }
            if (!keywords.get(i).add) {
                rs = TblItemKeyword.query("ITEM_ID, KEYWORD_ID", "KEYWORD_ID=" + keyword.key, null);
                if (rs.next()) {
                    break;
                } else {
                    TblKeyword.remove(keyword.key);
                }
            }
        }
    }

    /**
     * Annotate the items in this directory. If the item is, itself, a directory
     * annotateItems() is called recursively.
     *
     * @param dir the directory being processes
     * @throws AppFatal something went fatally wrong
     * @throws SQLException a database problem (should never occur)
     */
    private void annotateItemsByFilename(Path dir) throws AppFatal, SQLException {

        // go through the items in the directory
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path entry : ds) {

                // recurse if the item is a directory
                if (entry.toFile().isDirectory()) {
                    annotateItemsByFilename(entry);

                    // otherwise change the status and/or description   
                } else {
                    if (annotateItem(getFileName(entry))) {
                        count++;
                    }
                }
            }
        } catch (DirectoryIteratorException | IOException e) {
            throw new AppFatal(e.getMessage());
        }
    }

    /**
     * Process the file. It is assumed that the file is a CSV or TSV file with
     * one Item specified per line. The minimum is a specification as to which
     * column will contain the filename of the Item. An option is a boolean test
     * over information in the other columns to determine if the line is
     * successful. Optionally lines may be skipped at the head of the file.
     *
     * @param dir the directory being processes
     * @throws AppFatal something went fatally wrong
     * @throws SQLException a database problem (should never occur)
     */
    private void annotateItemsByFile(Path file, List<MatchPattern> patterns) throws AppError, AppFatal, SQLException {
        FileInputStream fis;
        InputStreamReader isr;
        BufferedReader br;
        String line;
        String[] tokens;

        // read control inputFile
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
                    if (tokens.length < fileColumn + 1) {
                        throw new AppError("Line does not contain enough columns to have the file name");
                    }
                    annotateItem(tokens[fileColumn]);
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
     * Annotate an item. This involves adding the description (if present),
     * changing the state (if required), adding any keywords specified, removing
     * any keywords specified, and adding an annotate event.
     *
     * @param itemName the name of the item being annotated
     * @return true if annotation succeeded
     * @throws AppFatal something went fatally wrong
     * @throws SQLException a database problem (should never occur)
     */
    private boolean annotateItem(String itemName) throws AppFatal, SQLException {
        int itemKey, instanceKey;
        ResultSet rsItem;
        int i;
        Keyword k;
        boolean stateChanged;

        // if item exist annotate it, otherwise complain
        rsItem = TblItem.findItem(itemName, null);
        if (rsItem.next()) {
            itemKey = TblItem.getItemId(rsItem);
            assert itemKey != 0;

            //update the state
            String stateMove = TblItem.getState(rsItem) + stateChange;
            stateChanged = true;
            switch (stateMove) {
                case "PX":  // no state change in keywords specified
                case "AX":
                case "CX":
                case "PP":  // state change to current state
                case "AA":
                case "CC":
                    break;
                case "CA":  // not allowed to change state directly from custody accepted to abandoned
                    stateChanged = false;
                    break;
                case "PA":  // processing to abandoned
                    TblItem.setState(itemKey, "A");
                    break;
                case "PC":  // processing or abandoned to custody accepted
                case "AC":
                    System.out.println("Here");
                    TblItem.setState(itemKey, "C");
                    break;
                case "AP":  // abandoned or custody accepted to processing
                case "CP":
                    TblItem.setState(itemKey, "P");
                    break;
                default:
                    LOG.log(Level.WARNING, "Undefined state change: ''{0}''", new Object[]{stateMove});
                    break;
            }

            // go through list of keywords to be added or removed. Add or remove
            // the linkage between the item and the keyword in the ItemKeyword
            // table. Note that if the keyword key is 0 and we are adding,
            // add the keyword in the keyword table first.
            for (i = 0; i < keywords.size(); i++) {
                k = keywords.get(i);
                if (k.keyword.equalsIgnoreCase("custody-accepted") || k.keyword.equalsIgnoreCase("abandoned")) {
                    continue;
                }
                if (k.add) {
                    if (k.key == 0) {
                        k.key = TblKeyword.add(k.keyword);
                        assert k.key > 0;
                    }
                    TblItemKeyword.add(itemKey, k.key);
                } else {
                    assert k.key > 0;
                    TblItemKeyword.remove(itemKey, k.key);
                }
            }

            // add the event (which includes the description)
            instanceKey = TblItem.getActiveInstanceId(rsItem);
            assert instanceKey != 0;
            if (stateChanged) {
                if (eventKey1 == 0) {
                    eventKey1 = TblEvent.add(desc1);
                }
                TblInstanceEvent.add(instanceKey, eventKey1);
            } else {
                if (eventKey2 == 0) {
                    eventKey2 = TblEvent.add(desc2);
                }
                TblInstanceEvent.add(instanceKey, eventKey2);
            }
            LOG.log(Level.FINE, "Annotated ''{0}''", new Object[]{itemName});
            return true;
        } else {
            LOG.log(Level.WARNING, "Failed annotating ''{0}'' as it was not in the database", new Object[]{itemName});
            return false;
        }
    }

    /**
     * Get a file name from a path. Suppress the final '.lnk' in a Windows short
     * cut. Replace single quotes with two quotes to be SQL safe.
     *
     * @param p
     * @return
     */
    private String getFileName(Path p) {
        String s;

        assert p != null;
        s = p.getFileName().toString().trim();
        if (s.toLowerCase().endsWith(".lnk")) {
            s = s.substring(0, s.length() - 4);
        } else if (s.toLowerCase().endsWith(" - shortcut")) {
            s = s.substring(0, s.length() - 11);
        }
        s = s.replace("'", "''");
        return s;
    }

    /**
     * Describe the proposed additions/deletions to keywords.
     *
     * @param doing true keywords being added, false if being removed
     * @return
     */
    private String keywordChanges(boolean add) {
        int i, count;
        StringBuilder sb = new StringBuilder();

        // how many keywords have been added/removed?
        count = 0;
        for (i = 0; i < keywords.size(); i++) {
            if (keywords.get(i).add == add) {
                count++;
            }
        }

        // if no keywords to report on, just return
        if (count == 0) {
            return (null);
        }

        // otherwise report on the keywords
        if (count == 1) {
            sb.append("Keyword ");
        } else {
            sb.append("Keywords ");
        }
        if (add) {
            sb.append(" added: ");
        } else {
            sb.append(" removed: ");
        }
        for (i = 0; i < keywords.size(); i++) {
            sb.append("'");
            sb.append(keywords.get(i).keyword);
            sb.append("'");
            if (count > 2) {
                sb.append(", ");
            } else if (count == 2) {
                sb.append(", & ");
            }
            count--;
        }
        sb.append(". ");
        return sb.toString();
    }
}
