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
 * This command annotates Items. There are two ways of specifying the Items to
 * be annotated, and two annotation methods. The result is complex!
 * 
 * Items can be annotated can be specified by either collecting them together
 * in a directory in the file system, or by naming them in a TSV or CSV file.
 * If they are collected together in a directory (actually the subtree under a
 * directory), the user needs to specify the root directory. If they are named
 * in a CSV or TSV file, the user needs to specify the name of the file, and
 * the column in the file that contains the Item name. Optionally they can also
 * specify whether the file is a CSV or TSV (the default is to use the file
 * extension to decide), a number of header lines to skip, and a set of patterns
 * to select particular lines in the file with Items to be annotated.
 * 
 * The user can use one or both of two annotation methods: a textual annotation,
 * and setting or removing keywords on Items. Both annotation methods generate
 * an Event that is associated with the Item. Keywords have the additional
 * property that they are sticky; they stay associated with the Item and users
 * can then generate reports listing Items with particular keywords.
 * 
 * Keywords are also used to implement a simple state diagram for Items. Items
 * start out in the 'Processing' state. If the keywords 'Custody-accepted' or
 * 'Abandoned' are then added to at Item, it moves into the 'Custody-accepted'
 * or 'Abandoned' state. The Item is then considered to be finalised, and
 * subsequent operations on it are limited. If necessary, these keywords can
 * be removed and the Item moves back to the Processing state). If
 * 'Custody-accepted' is added to an Item in the Abandoned state, the
 * 'Abandoned' keyword is removed and the Item moves to the Custody-accepted
 * state. You can't, however, move directly from the Custody-accepted state
 * to the Abandoned state.
 *
 * @author Andrew Waugh
 */
public class CmdAnnotate extends Command {

    private final static Logger LOG = Logger.getLogger("TrackTransfer.CmdAnnotate");
    
    // variables specifying the annotation
    private String desc;        // description specified by the user
    private String desc1;       // description of the event assuming all the state changes are valid
    private int eventKey1;      // event matching desc1 (this will only be populated when an item that needs it is encountered)
    private String desc2;       // description of the event if couldn't change from Custody-accepted to Abandoned
    private int eventKey2;      // event matching desc2 (this will only be populated when an Item that needs it is encountered)
    private final List<Keyword> keywords; // keywords to be changed in the selected Items
    private String stateChange; // stateChange of Items 'X'=unchanged, 'P'=to processing, 'A'=to abandoned, 'C'=to custody accepted
    
    // variables specifying the Items
    private Path rootDir;       // root directory containing the objects being annotated (if items identified by a directory)
    private Path inputFile;     // inputFile to process for Item names & condition (if items identified by a inputFile)
    private int skip;           // number of lines in the header to skip
    private List<MatchPattern> patterns; // patterns to match agains C/TSV files
    private int fileColumn;     // column in which to find the filename
    private boolean forceCSV;   // true if forcing the file to be a CSV file
    private boolean forceTSV;   // true if forcing the file to be a TSV file
    private boolean csv;        // true if file is being treated as a CSV file
    private int count;          // number of items annotated
    private String usage = "[-db <database>] [-desc <text>] [-set <keyword>] [-remove <keyword>] [-custody-accepted] [-abandoned] [-dir <directory>] [[-in] file [-skip <count>] [-csv] [-tsv] [-pattern <pattern>] [itemcol <column>]] [-v] [-d] [-help]";

    public CmdAnnotate() throws AppFatal {
        super();
        keywords = new ArrayList<>();
    }

    /**
     * Annotate Items selected by being in a directory (or the tree under the
     * directory). The database is optional, if null the '.mv.db' directory in
     * the current working directory is used. The rootDir must be present and
     * a directory. Any of the description, keywordsToAdd, and keywordsToRemove
     * can be null, but not all of them.
     * 
     * Do not call this method directly, use the wrapper in the TrackTransfer
     * class.
     *
     * @param database the string representing the database
     * @param rootDir the root of the tree of items to be annotated
     * @param description a description of this delivery (e.g. an ID)
     * @param keywordsToAdd list of keywords to add to selected Items
     * @param keywordsToRemove list of keywords to removed from selected Items
     * @param veoOnly true if only files ending in .veo or .veo.zip are to be processed
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     * @throws SQLException SQL problem occurred
     */
    public void annotateItemsByDirectory(
            String database, Path rootDir, String description, List<String> keywordsToAdd, List<String> keywordsToRemove, boolean veoOnly)
            throws AppFatal, AppError, SQLException {
        int i;

        assert rootDir != null;
        assert description != null && keywordsToAdd.isEmpty() && keywordsToRemove.isEmpty();

        // set up for the run
        count = 0;
        keywords.clear();
        stateChange = "X";
        eventKey1 = 0;
        eventKey2 = 0;

        this.database = database;
        this.desc = description;
        this.rootDir = rootDir;
        this.veo = veoOnly;
        for (i = 0; i < keywordsToRemove.size(); i++) {
            removeKeyword(keywordsToRemove.get(i));
        }
        for (i = 0; i < keywordsToAdd.size(); i++) {
            setKeyword(keywordsToAdd.get(i));
        }

        // parameters for annotating by file are not set
        inputFile = null;
        skip = 0;
        patterns = null;
        fileColumn = 0;
        csv = false;

        // test the user has specified everything & do it.
        testParameters();
        doIt();
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
     * Do not call this method directly, use the wrapper in the TrackTransfer
     * class.
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
     * @throws SQLException SQL problem occurred
     */
    public void annotateItemsByFile(
            String database, Path file, boolean forceCSV, boolean forceTSV, int skip, int fileColumn,
            String patterns, String description, List<String> keywordsToAdd, List<String> keywordsToRemove,
            boolean veoOnly) throws AppFatal, AppError, SQLException {
        int i;

        assert file != null;
        assert fileColumn > 0;

        count = 0;
        keywords.clear();
        stateChange = "X";
        eventKey1 = 0;
        eventKey2 = 0;

        this.database = database;
        this.desc = description;
        this.inputFile = file;
        this.skip = skip;
        this.fileColumn = fileColumn;
        csv = isCSVFile(file, forceCSV, forceTSV);
        this.patterns = MatchPattern.parse(patterns);
        this.veo = veoOnly;
        if (keywordsToRemove != null) {
            for (i = 0; i < keywordsToRemove.size(); i++) {
                removeKeyword(keywordsToRemove.get(i));
            }
        }
        if (keywordsToAdd != null) {
            for (i = 0; i < keywordsToAdd.size(); i++) {
                setKeyword(keywordsToAdd.get(i));
            }
        }

        // parameters for annotating by directory are not set
        rootDir = null;

        testParameters();
        doIt();
    }

    /**
     * Annotate items. Command line version.
     *
     * @param args
     * @throws AppFatal thrown if TrackTransfer had an internal error
     * @throws AppError thrown if the calling program did something wrong
     * @throws SQLException SQL problem occurred
     */
    public void annotateItems(String args[]) throws AppFatal, AppError, SQLException {
        String s;

        count = 0;
        keywords.clear();
        stateChange = "X";
        eventKey1 = 0;
        eventKey2 = 0;

        // getting items from a CSV or TSV inputFile
        skip = 0;
        fileColumn = -1;
        forceCSV = false;
        forceTSV = false;
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
        
        // test to see if the request is sensible
        testParameters();

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

        doIt();

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
                setKeyword(args[i]);
                i++;
                j = 2;
                break;
            // custody has been accepted of the items
            case "-custody-accepted":
                setKeyword("Custody-accepted");
                i++;
                j = 1;
                break;
            // items have been abandoned
            case "-abandoned":
                setKeyword("Abandoned");
                i++;
                j = 1;
                break;
            // remove a keyword from the items
            case "-remove":
                i++;
                removeKeyword(args[i]);
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
                forceCSV = true;
                j = 1;
                break;
            // inputFile is a TSV inputFile
            case "-tsv":
                i++;
                forceTSV = true;
                j = 1;
                break;
            // otherwise complain
            default:
                j = 0;
        }
        return j;
    }
    
    /**
     * Test that the user has fully specified what they want to do.
     * 
     * @throws AppError 
     */
    private void testParameters() throws AppError {
        
        // check we are actually making an annotation
        if (desc == null && keywords.isEmpty()) {
            throw new AppError("At least one of '-desc', '-set', or '-remove' must be set ");
        }
        
        // check one of a root directory or an input file (but not both) is specified
        if ((rootDir != null && inputFile != null) || (rootDir == null && inputFile == null)) {
            throw new AppError("Must specify either the directory containing the Items being annotated (-dir) OR a TSV/CSV file containing the Item names (-in)");
        }
        
        // check specific details if getting Items from a CSV/TSV inputFile
        if (inputFile != null) {
            if (fileColumn < 0) {
                throw new AppError("When reading Items from a file, the column in which the filename (Item name) is to be found must be specified (-filename)");
            }
            if (skip < 0) {
                throw new AppError("When reading Items from a file, the skip count must be zero or a positive number (-skip)");
            }
            if (!inputFile.toFile().exists()) {
                throw new AppError("Input file '" + inputFile.toString() + "' does not exist");
            }
            if (!inputFile.toFile().isFile()) {
                throw new AppError("Input file '" + inputFile.toString() + "' is not a directory");
            }
            csv = isCSVFile(inputFile, forceCSV, forceTSV);
        }

        // if getting Items from a directory, check if the root directory exists and is a directory
        if (rootDir != null) {
            if (!rootDir.toFile().exists()) {
                throw new AppError("Root directory '" + rootDir.toString() + "' does not exist");
            }
            if (!rootDir.toFile().isDirectory()) {
                throw new AppError("Root directory '" + rootDir.toString() + "' is not actually a directory");
            }
        }
    }

    /**
     * Decide whether this file will be parsed as a CSV or TSV file. If the user
     * has specified its type (forceCSV or forceTSV), use that. Otherwise, if
     * the file type is a clear hint (.csv or .tsv), use that. If all else fails
     * assume a TSV and hope for the best.
     *
     * @param file path of file to parse
     * @param forceCSV true if user said it was a CSV file
     * @param forceTSV true if user said it was a TSV file
     * @return
     */
    private boolean isCSVFile(Path file, boolean forceCSV, boolean forceTSV) {
        String filename;

        assert file != null;
        if (forceCSV) {
            return true;
        } else if (forceTSV) {
            return false;
        } else {
            filename = file.getFileName().toString();
            if (filename.toLowerCase().endsWith(".csv")) {
                return true;
            } else if (filename.toLowerCase().endsWith(".tsv")) {
                return false;
            } else {
                return false;
            }
        }
    }

    /**
     * Internal function that actually does the work.
     *
     * @throws AppFatal
     * @throws AppError
     * @throws SQLException
     */
    private void doIt() throws AppFatal, AppError, SQLException {
        StringBuilder sb = new StringBuilder();
        String s;

        // Append to the description the user gave (if any) details about the
        // keywords to be added or removed, and the final state of the Items.
        // Make a secondary description for the situation where the existing
        // state of an Item is 'Custody-accepted' and the user wants to change
        // it Abandoned (not allowed, no state change).
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
        } else if (inputFile != null) {
            annotateItemsByFile(inputFile, patterns);
        }

        // remove any keywords that are no longer referenced
        removeDeadKeywords();

        disconnectDB();
    }

    /**
     * Set a keyword, paying attention to the special keywords that change state
     *
     * @param keyword
     */
    private void setKeyword(String keyword) {
        if (keyword.equalsIgnoreCase("Custody-accepted")) {
            stateChange = "C";
            addKeyword(keyword, true, true);
        } else if (keyword.equalsIgnoreCase("Abandoned")) {
            stateChange = "A";
            addKeyword(keyword, true, true);
        } else {
            addKeyword(keyword, true, false);
        }
    }

    /**
     * Remove a keyword, paying attention to the special keywords that change
     * state
     *
     * @param keyword
     */
    private void removeKeyword(String keyword) {
        if (keyword.equalsIgnoreCase("Custody-accepted") || keyword.equalsIgnoreCase("Abandoned")) {
            stateChange = "P";
            addKeyword(keyword, false, true);
        } else {
            addKeyword(keyword, false, false);
        }

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
     * Remove keywords that are no longer referenced by any Items. This occurs if
     * we removed keywords from items, and this removed the last reference to a
     * keyword.
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
                if (!rs.next()) {
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
     * Process the file. The file must be a CSV or TSV file with
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
                // annotate the specified Item if no pattern is specified, or
                // the line matches the pattern
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
            throw new AppError("Failed to open input file '" + file.toString() + "'" + e.toString());
        } catch (IOException e) {
            throw new AppError("Failed reading the input file '" + file.toString() + "'" + e.toString());
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
}
