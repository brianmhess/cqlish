package hessian.cqlish;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryValidationException;
import org.cassandraunit.utils.RestartableEmbeddedCassandraServerHelper;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.history.MemoryHistory;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class CqlishApplication {
    private String version = "0.0.2";
    private Session session;
    private CodecRegistry codecRegistry;
    private String scriptFile;
    private ConsoleReader reader;
    private String cql;
    private boolean resetCassandra = false;

    public static String usage() {
        return "cqlish [-reset <true/false>] [-f <scriptfile>]" +
                " Options:" +
                "   -f <scriptfile>       Will execute CQL commands from script file." +
                "                           One CQL command per line." +
                "                           Comment lines beginning with #." +
                "   -reset <true/false>   If reset is true then all data/tables/keyspaces will be reset." +
                "                           Default is false.";
    }

    public static void main(String[] args) throws Exception {
        CqlishApplication cqlishApplication = new CqlishApplication();
        boolean success = cqlishApplication.run(args);
        if (success) {
            System.exit(0);
        } else {
            System.err.println("There was an error");
            System.exit(-1);
        }
    }

    public boolean parseArgs(String[] args) {
        String tkey;
        if (0 != args.length % 2) {
            System.err.println("Must specify an even number of arguments");
            return false;
        }

        Map<String, String> amap = new HashMap<String, String>();
        for (int i = 0; i < args.length; i += 2) {
            amap.put(args[i], args[i + 1]);
        }
        if (null != (tkey = amap.remove("-f")))     scriptFile = tkey;
        if (null != (tkey = amap.remove("-reset"))) resetCassandra = Boolean.parseBoolean(tkey);

        return validateArgs();
    }

    public boolean validateArgs() {
        if (null != scriptFile) {
            File tfile = new File(scriptFile);
            if (!tfile.isFile()) {
                System.out.println("Script file must be a file (" + scriptFile + ")");
                return false;
            }
        }

        return true;
    }

    public boolean extractLibs() {
        String[] libs = {
                "libsigar-amd64-freebsd-6.so",
                "libsigar-amd64-linux.so",
                "libsigar-amd64-solaris.so",
                "libsigar-ia64-hpux-11.sl",
                "libsigar-ia64-linux.so",
                "libsigar-pa-hpux-11.sl",
                "libsigar-ppc-aix-5.so",
                "libsigar-ppc-linux.so",
                "libsigar-ppc64-aix-5.so",
                "libsigar-ppc64-linux.so",
                "libsigar-ppc64le-linux.so",
                "libsigar-s390x-linux.so",
                "libsigar-sparc-solaris.so",
                "libsigar-sparc64-solaris.so",
                "libsigar-universal-macosx.dylib",
                "libsigar-universal64-macosx.dylib",
                "libsigar-x86-freebsd-5.so",
                "libsigar-x86-freebsd-6.so",
                "libsigar-x86-linux.so",
                "libsigar-x86-solaris.so",
                "sigar-amd64-winnt.dll",
                "sigar-x86-winnt.dll",
                "sigar-x86-winnt.lib"};
        String target = System.getProperty("user.dir") + "/target/libs";
        RestartableEmbeddedCassandraServerHelper.mkdir(target);
        for (String lib : libs) {
            InputStream is = this.getClass().getResourceAsStream("/libs/" + lib);
            try {
                Files.copy(is, Paths.get(target, lib), StandardCopyOption.REPLACE_EXISTING);
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
                return false;
            }
        }
        try {
            System.setProperty("java.library.path", target);
            Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
            fieldSysPath.setAccessible(true);
            fieldSysPath.set(null, null);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        if (null == System.getProperty("cassandra.jmx.local.port"))
            System.setProperty("cassandra.jmx.local.port", "7199");

        return true;
    }

    public boolean setup() throws Exception {
        reader = new ConsoleReader();
        reader.setHandleUserInterrupt(true);
        reader.setHistory(new MemoryHistory());
        reader.setHistoryEnabled(true);
        printSplash();

        if (!extractLibs())
            return false;
        reader.print(colorWrap(ANSI_YELLOW, "Starting embedded Cassandra... "));
        if (resetCassandra)
            RestartableEmbeddedCassandraServerHelper.rmdir(RestartableEmbeddedCassandraServerHelper.DEFAULT_TMP_DIR);
        RestartableEmbeddedCassandraServerHelper.startEmbeddedCassandra();
        session = RestartableEmbeddedCassandraServerHelper.getSession();
        reader.println(colorWrap(ANSI_GREEN, " started"));
        reader.flush();
        codecRegistry = session.getCluster().getConfiguration().getCodecRegistry();

        return true;
    }

    public boolean run(String[] args) throws Exception {
        if (!parseArgs(args)) {
            System.err.println(usage());
            return false;
        }
        if (!setup())
            return false;
        if (!processScriptFile(scriptFile))
            return false;

        return doRepl();
    }

    public void help() throws IOException {
        String help = " Enter CQL and end the CQL statment with a semicolon ';'.\n" +
                " You can have multi-line CQL statements, just hit Enter mid-statement\n" +
                " You end the statement with a semicolon.\n" +
                " CTRL-C will clear the current CQL statement.\n" +
                " Some cqlish commands (case does not matter):\n" +
                "   HELP            this message\n" +
                "   EXIT, QUIT      exits cqlish\n" +
                "   CLEAR           clears the screen\n" +
                "   SOURCE <file>   executes the CQL commands in the supplied file\n";
        reader.println(colorWrap(ANSI_YELLOW, help));
        reader.flush();
    }

    public void info() throws IOException {
        String info = " Info:\n" +
                "   Version: " + version + "\n" +
                "   Terminal: " + reader.getTerminal().toString() + "\n" +
                "   Terminal supports ANSI: " + reader.getTerminal().isAnsiSupported() + "\n" +
                "   Terminal output encoding: " + reader.getTerminal().getOutputEncoding() + "\n";
        reader.println(colorWrap(ANSI_YELLOW, info));
        reader.flush();
    }

    public String colorWrap(String color, String string) {
        String prefix = reader.getTerminal().isAnsiSupported() ? color : "";
        String suffix = reader.getTerminal().isAnsiSupported() ? ANSI_RESET : "";
        return prefix + string + suffix;
    }

    public String firstPrompt() {
        String firstPrompt = "cqlish:";
        if (null != session.getLoggedKeyspace())
            firstPrompt = firstPrompt + session.getLoggedKeyspace();
        return firstPrompt + "> ";
    }

    public String continuedPrompt() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < firstPrompt().length() - 5; i++)
            sb.append(" ");
        sb.append("===> ");
        return sb.toString();
    }

    public void printSplash() throws IOException {
        String[][] colors = {
                {ANSI_WHITE, ANSI_WHITE, ANSI_WHITE, ANSI_WHITE, ANSI_WHITE, ANSI_WHITE},
                {ANSI_RED, ANSI_RED, ANSI_RED, ANSI_RED, ANSI_RED, ANSI_RED},
                {ANSI_YELLOW, ANSI_YELLOW, ANSI_YELLOW, ANSI_YELLOW, ANSI_YELLOW, ANSI_YELLOW},
                {ANSI_GREEN, ANSI_GREEN, ANSI_GREEN, ANSI_GREEN, ANSI_GREEN, ANSI_GREEN},
                {ANSI_CYAN, ANSI_CYAN, ANSI_CYAN, ANSI_CYAN, ANSI_CYAN, ANSI_CYAN},
                {ANSI_BLUE, ANSI_BLUE, ANSI_BLUE, ANSI_BLUE, ANSI_BLUE, ANSI_BLUE},
                {ANSI_MAGENTA, ANSI_MAGENTA, ANSI_MAGENTA, ANSI_MAGENTA, ANSI_MAGENTA, ANSI_MAGENTA},
                {ANSI_RED, ANSI_YELLOW, ANSI_GREEN, ANSI_CYAN, ANSI_BLUE, ANSI_MAGENTA},
                {ANSI_BLUE, ANSI_BLUE, ANSI_CYAN, ANSI_CYAN, ANSI_WHITE, ANSI_WHITE}
        };
        int color = (int)(System.currentTimeMillis() % colors.length);
        reader.println("" +
                colors[color][0] +     "              ___      __  \n" +
                colors[color][1] +  "  _________ _/ (_)____/ /_ \n" +
                colors[color][2] +   " / ___/ __ `/ / / ___/ __ \\\n" +
                colors[color][3] +    "/ /__/ /_/ / / (__  ) / / /\n" +
                colors[color][4] +    "\\___/\\__, /_/_/____/_/ /_/ \n" +
                colors[color][5] + "       /_/                 " +
                ANSI_RESET);
        reader.println("Welcome to cqlish (version " + version + "). Type HELP for some help\n");
        reader.flush();
    }

    public boolean doRepl() throws Exception {
        cql = "";
        Writer writer = reader.getOutput();
        while (true) {
            String prompt = colorWrap(ANSI_CYAN, cql.isEmpty() ? firstPrompt() : continuedPrompt());
            String line = null;

            try {
                line = reader.readLine(prompt);
            }
            catch (UserInterruptException uie) {
                cql = "";
                continue;
            }
            if (null == line) {
                break;
            }

            line = line.trim();

            if (line.equalsIgnoreCase("quit")
                    || line.equalsIgnoreCase("quit;")
                    || line.equalsIgnoreCase("exit")
                    || line.equalsIgnoreCase("exit;")) {
                break;
            }

            if (line.equalsIgnoreCase("help")
                    || (line.equalsIgnoreCase("help;"))) {
                help();
                continue;
            }

            if (line.equalsIgnoreCase("info")
                    || (line.equalsIgnoreCase("info;"))) {
                info();
                continue;
            }

            if (line.equalsIgnoreCase("clear")
                    || (line.equalsIgnoreCase("clear;"))) {
                reader.clearScreen();
                continue;
            }

            cql = cql + " " + line;
            if (!cql.endsWith(";")) {
                continue;
            }

            executeAndPrintCql(session, cql);
            cql = "";
        }

        reader.println(colorWrap(ANSI_YELLOW, "\nExiting...."));
        reader.flush();
        return true;
    }

    public boolean processScriptFile(String file) throws IOException {
        if (null == file)
            return true;

        File infile = new File(file);
        Scanner scanner;
        try {
            scanner = new Scanner(new FileInputStream(infile));
        } catch (FileNotFoundException fe) {
            reader.println(colorWrap(ANSI_RED, "ERROR: Could not find file " + file));
            reader.flush();
            return false;
        }

        reader.println(colorWrap(ANSI_YELLOW,"Processing scriptfile " + file + ":"));
        reader.flush();
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            line = line.trim();
            if (line.startsWith("#"))
                continue;
            executeCql(session, line);
        }
        reader.println(colorWrap(ANSI_YELLOW, "Finished processing scriptfile"));
        reader.flush();
        return true;
    }

    public ResultSet executeCql(Session session, String cql) throws IOException {
        reader.println(colorWrap(ANSI_YELLOW, " ==> " + cql));
        reader.flush();
        ResultSet resultSet;
        try {
            resultSet = session.execute(cql);
        } catch (QueryValidationException qve) {
            reader.println(colorWrap(ANSI_RED, "Invalid Query: " + qve.getMessage()));
            reader.flush();
            return null;
        }

        return resultSet;
    }

    public void executeAndPrintCql(Session session, String cql) throws IOException {
        cql = cql.trim();
        if (handleSpecialCommands(cql))
            return;
        long begin = System.currentTimeMillis();
        ResultSet resultSet = executeCql(session, cql);
        long end = System.currentTimeMillis();
        long elapsed = end - begin;

        // Handle error
        if (null == resultSet)
            return;

        if (resultSet.isExhausted()) {
            reader.println("Ok");
            reader.flush();
        } else {
            prettyPrint(resultSet);
        }
        reader.println("\n Elapsed time: " + elapsed + " ms\n");
        reader.flush();
    }

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_MAGENTA = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public void prettyPrint(ResultSet resultSet) throws IOException {
        String header_color_begin = (reader.getTerminal().isAnsiSupported()) ? ANSI_GREEN : "";
        String null_color_begin = (reader.getTerminal().isAnsiSupported()) ? ANSI_MAGENTA : "";
        String color_reset = (reader.getTerminal().isAnsiSupported()) ? ANSI_RESET : "";
        List<ColumnDefinitions.Definition> cdefs = resultSet.getColumnDefinitions().asList();
        int numCols = cdefs.size();
        List<String> columnNames = new ArrayList<String>(numCols);
        List<Integer> longest = new ArrayList<Integer>(numCols);
        for (int i = 0; i < numCols; i++) {
            String colname = cdefs.get(i).getName();
            columnNames.add(colname);
            longest.add(colname.length());
        }

        List<List<String>> rows = new ArrayList<List<String>>();
        for (Row r : resultSet) {
            List<String> row = new ArrayList<String>(numCols);
            for (int i = 0; i < numCols; i++) {
                String cell = r.isNull(i) ? null : codecRegistry.codecFor(cdefs.get(i).getType()).format(r.get(i, codecRegistry.codecFor(cdefs.get(i).getType())));
                int len = r.isNull(i) ? 4 : cell.length();
                if (len > longest.get(i))
                    longest.set(i, len);
                row.add(cell);
            }
            rows.add(row);
        }

        StringBuilder fmt2 = new StringBuilder(" " + header_color_begin + "%" + longest.get(0) + "." + longest.get(0) + "s" + color_reset + " ");
        StringBuilder sepline = new StringBuilder();
        for (int j = 0; j < longest.get(0) + 2; j++)
            sepline.append("-");
        for (int i = 1; i < longest.size(); i++) {
            fmt2.append("| " + header_color_begin + "%" + longest.get(i) + "." + longest.get(i) + "s" + color_reset + " ");
            sepline.append("+");
            for (int j = 0; j < longest.get(i) + 2; j++)
                sepline.append("-");
        }

        reader.println(String.format(fmt2.toString(), columnNames.toArray()));
        reader.println(sepline.toString());
        for (int i = 0; i < rows.size(); i++) {
            List<String> row = rows.get(i);
            StringBuilder fmt = new StringBuilder();
            if (null == row.get(0))
                fmt.append(" " + null_color_begin + "%" + longest.get(0) + "." + longest.get(0) + "s " + color_reset);
            else
                fmt.append(" "                 + "%" + longest.get(0) + "." + longest.get(0) + "s ");
            for (int j = 1; j < longest.size(); j++) {
                if (null == row.get(j))
                    fmt.append("| " + null_color_begin + "%" + longest.get(j) + "." + longest.get(j) + "s " + color_reset);
                else
                    fmt.append("| "                 + "%" + longest.get(j) + "." + longest.get(j) + "s ");
            }
            reader.println(String.format(fmt.toString(), rows.get(i).toArray()));
        }
        reader.flush();
    }

    public boolean handleSpecialCommands(String input) throws IOException {
        input = input.substring(0, input.length()-1);
        String[] pieces = input.split("\\s+");
        String cmd = pieces[0];
        if ((cmd.equalsIgnoreCase("desc")) || (cmd.equalsIgnoreCase("describe"))) {
            return handleDescribe(input, pieces);
        }
        if (cmd.equalsIgnoreCase("source")) {
            return handleSource(input, pieces);
        }
        return false;
    }

    public boolean handleDescribe(String input, String[] pieces) throws IOException {
        if (pieces.length < 2) {
            reader.println(colorWrap(ANSI_RED, "ERROR: bad describe: " + input));
            reader.flush();
            return true;
        }
        if (pieces[1].equalsIgnoreCase("keyspaces")) {
            describeKeyspaces();
            return true;
        }
        if (pieces[1].equalsIgnoreCase("tables")) {
            if (pieces.length > 2) {
                describeTables(pieces[2]);
                return true;
            }
            else {
                if (null != session.getLoggedKeyspace()) {
                    describeTables(session.getLoggedKeyspace());
                    return true;
                }
                else {
                    reader.println(colorWrap(ANSI_RED, "ERROR: must specify keyspace to list tables"));
                    reader.flush();
                    return true;
                }
            }
        }
        if (pieces[1].equalsIgnoreCase("table")) {
            if (pieces.length < 3) {
                reader.println(colorWrap(ANSI_RED, "ERROR: must specify table"));
                reader.flush();
                return true;
            }
            else {
                if (pieces.length > 3) {
                    describeTable(pieces[2], pieces[3]);
                    return true;
                }
                else {
                    if (pieces[2].contains(".")) {
                        describeTable(pieces[2]);
                        return true;
                    }
                    else {
                        String keyspace = session.getLoggedKeyspace();
                        if (null == keyspace) {
                            reader.println(colorWrap(ANSI_RED, "ERROR: bad describe table command: " + input));
                            reader.flush();
                            return true;
                        }
                        describeTable(keyspace, pieces[2]);
                        return true;
                    }
                }
            }
        }
        reader.println(colorWrap(ANSI_RED, "ERROR: bad describe command: " + input));
        reader.flush();
        return true;
    }

    public void describeKeyspaces() throws IOException {
        reader.println(colorWrap(ANSI_YELLOW, " ==> DESCRIBE KEYSPACES"));
        for (KeyspaceMetadata km : session.getCluster().getMetadata().getKeyspaces()) {
            reader.println(" " + km.getName());
        }
        reader.println();
        reader.flush();
    }

    public void describeTables(String keyspace) throws IOException {
        if (null == session.getCluster().getMetadata().getKeyspace(keyspace))
            reader.println(colorWrap(ANSI_RED, "ERROR: keyspace (" + keyspace + ") not found"));
        reader.println(colorWrap(ANSI_YELLOW, " ==> DESCRIBE TABLES"));
        for (TableMetadata tm : session.getCluster().getMetadata().getKeyspace(keyspace).getTables()) {
            reader.println(" " + tm.getName());
        }
        reader.println();
        reader.flush();
    }

    public void describeTable(String keyspace, String table) throws IOException {
        KeyspaceMetadata km = session.getCluster().getMetadata().getKeyspace(keyspace);
        if (null == km) {
            reader.println(colorWrap(ANSI_RED, "ERROR: keyspace (" + keyspace + ") not found"));
            reader.flush();
            return;
        }
        TableMetadata tm = km.getTable(table);
        if (null == tm) {
            reader.println(colorWrap(ANSI_RED, "ERROR: table (" + keyspace + "." + table + ") not found"));
            reader.flush();
            return;
        }
        reader.println(colorWrap(ANSI_YELLOW, " ==> DESCRIBE TABLE"));
        reader.println(tm.exportAsString());
        reader.println();
        reader.flush();
    }

    public void describeTable(String keyspacetable) throws IOException {
        String[] pieces = keyspacetable.split("\\.");
        if (pieces.length != 2) {
            reader.println(colorWrap(ANSI_RED, "ERROR: poorly formatted table (" + keyspacetable + ")"));
            reader.flush();
            return;
        }
        String keyspace = pieces[0];
        String table = pieces[1];
        describeTable(keyspace, table);
    }

    public boolean handleSource(String input, String[] pieces) throws IOException {
        if (2 > pieces.length) {
            reader.println(colorWrap(ANSI_RED, "ERROR: must supply filename"));
            reader.flush();
            return true;
        }
        String file = pieces[1];
        for (int i = 2; i < pieces.length; i++)
            file = file + " " + pieces[i];
        if (file.startsWith("'") && file.endsWith("'"))
            file = file.substring(1, file.length() - 1);
        if (file.startsWith("\"") && file.endsWith("\""))
            file = file.substring(1, file.length() - 1);

        processScriptFile(file);
        return true;
    }
}