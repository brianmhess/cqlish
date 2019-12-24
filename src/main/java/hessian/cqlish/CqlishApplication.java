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
    private Session session;
    private CodecRegistry codecRegistry;
    private String scriptFile;
    private ConsoleReader reader;
    private String cql;

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
        if (null != (tkey = amap.remove("-f"))) scriptFile = tkey;

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
        String target = System.getenv("user.dir") + "/target";
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
        if (!extractLibs())
            return false;
        System.out.print("Starting embedded Cassandra... ");
        RestartableEmbeddedCassandraServerHelper.startEmbeddedCassandra();
        session = RestartableEmbeddedCassandraServerHelper.getSession();
        System.out.println(" started");
        codecRegistry = session.getCluster().getConfiguration().getCodecRegistry();

        return true;
    }

    public boolean run(String[] args) throws Exception {
        if (!parseArgs(args))
            return false;
        if (!setup())
            return false;
        if (!processScriptFile())
            return false;

        return doRepl();
    }

    public void help() {
        System.out.println(" Enter CQL and end the CQL statment with a semicolon ';'.");
        System.out.println(" You can have multi-line CQL statements, just hit Enter mid-statement");
        System.out.println(" You end the statement with a semicolon.");
        System.out.println(" CTRL-C will clear the current CQL statement.");
        System.out.println(" To exit, type 'exit' or 'quit' (case does not matter)");
    }

    public boolean doRepl() throws Exception {
        reader = new ConsoleReader();
        reader.setHandleUserInterrupt(true);
        reader.setHistory(new MemoryHistory());
        reader.setHistoryEnabled(true);

        String firstPrompt = "cqlish> ";
        String continuedPrompt = "   ===> ";
        cql = "";
        while (true) {
            String prompt = cql.isEmpty() ? firstPrompt : continuedPrompt;
            String line;

            try {
                line = reader.readLine(prompt);
            }
            catch (UserInterruptException uie) {
                cql = "";
                continue;
            }
            line = line.trim();

            if (line.equalsIgnoreCase("quit")
                    || line.equalsIgnoreCase("quit;")
                    || line.equalsIgnoreCase("exit")
                    || line.equalsIgnoreCase("exit;"))
                break;

            if (line.equalsIgnoreCase("help")
                    || (line.equalsIgnoreCase("help;"))) {
                help();
                continue;
            }

            cql = cql + " " + line;
            if (!cql.endsWith(";")) {
                continue;
            }

            executeAndPrintCql(session, cql, System.out);
            cql = "";
        }

        return true;
    }

    public boolean processScriptFile() {
        if (null == scriptFile)
            return true;

        File infile = new File(scriptFile);
        Scanner scanner;
        try {
            scanner = new Scanner(new FileInputStream(infile));
        } catch (FileNotFoundException fe) {
            System.err.println("Could not find file " + scriptFile);
            return false;
        }

        System.out.println("Processing scriptfile");
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            line = line.trim();
            if (line.startsWith("#"))
                continue;
            executeCql(session, line, System.out);
        }
        System.out.println("Finished processing scriptfile");
        return true;
    }

    public ResultSet executeCql(Session session, String cql, PrintStream out) {
        out.println(" ==> \"" + cql + "\"");
        ResultSet resultSet;
        try {
            resultSet = session.execute(cql);
        } catch (QueryValidationException qve) {
            out.println("Invalid Query: " + qve.getMessage());
            out.flush();
            return null;
        }

        return resultSet;
    }

    public void executeAndPrintCql(Session session, String cql, PrintStream out) {
        cql = cql.trim();
        if (handleSpecialCommands(cql, out))
            return;
        long begin = System.currentTimeMillis();
        ResultSet resultSet = executeCql(session, cql, out);
        long end = System.currentTimeMillis();
        long elapsed = end - begin;

        // Handle error
        if (null == resultSet)
            return;

        if (resultSet.isExhausted()) {
            out.println("Ok");
            out.flush();
        } else {
            prettyPrint(resultSet, out);
        }
        out.println("\n Elapsed time: " + elapsed + " ms\n");
        out.flush();
    }

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    public void prettyPrint(ResultSet resultSet, PrintStream out) {
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
                String cell = codecRegistry.codecFor(cdefs.get(i).getType()).format(r.get(i, codecRegistry.codecFor(cdefs.get(i).getType())));
                if (cell.length() > longest.get(i))
                    longest.set(i, cell.length());
                row.add(cell);
            }
            rows.add(row);
        }

        StringBuilder fmt = new StringBuilder(" %" + longest.get(0) + "." + longest.get(0) + "s ");
        StringBuilder fmt2 = new StringBuilder(" " + ANSI_CYAN + "%" + longest.get(0) + "." + longest.get(0) + "s" + ANSI_RESET + " ");
        StringBuilder sepline = new StringBuilder();
        for (int j = 0; j < longest.get(0) + 2; j++)
            sepline.append("-");
        for (int i = 1; i < longest.size(); i++) {
            fmt.append("| %" + longest.get(i) + "." + longest.get(i) + "s ");
            fmt2.append("| " + ANSI_CYAN + "%" + longest.get(i) + "." + longest.get(i) + "s" + ANSI_RESET + " ");
            sepline.append("+");
            for (int j = 0; j < longest.get(i) + 2; j++)
                sepline.append("-");
        }

        System.out.println(String.format(fmt2.toString(), columnNames.toArray()));
        System.out.println(sepline.toString());
        for (int i = 0; i < rows.size(); i++)
            System.out.println(String.format(fmt.toString(), rows.get(i).toArray()));

    }


    public void prettyPrint(List<String> header, List<List<String>> rows, List<Integer> longest, PrintStream out) {
        StringBuilder fmt = new StringBuilder(" %" + longest.get(0) + "." + longest.get(0) + "s ");
        StringBuilder fmt2 = new StringBuilder(" " + ANSI_CYAN + "%" + longest.get(0) + "." + longest.get(0) + "s" + ANSI_RESET + " ");
        StringBuilder sepline = new StringBuilder();
        for (int j = 0; j < longest.get(0) + 2; j++)
            sepline.append("-");
        for (int i = 1; i < longest.size(); i++) {
            fmt.append("| %" + longest.get(i) + "." + longest.get(i) + "s ");
            fmt2.append("| " + ANSI_CYAN + "%" + longest.get(i) + "." + longest.get(i) + "s" + ANSI_RESET + " ");
            sepline.append("+");
            for (int j = 0; j < longest.get(i) + 2; j++)
                sepline.append("-");
        }

        System.out.println(String.format(fmt2.toString(), header.toArray()));
        System.out.println(sepline.toString());
        for (int i = 0; i < rows.size(); i++)
            System.out.println(String.format(fmt.toString(), rows.get(i).toArray()));

    }

    public void prettyPrint(List<String> header, List<List<String>> rows, PrintStream out) {
        List<Integer> longest = new ArrayList<Integer>(header.size());
        for (int i = 0; i < header.size(); i++)
            longest.add(header.get(i).length());
        for (int i = 0; i < rows.size(); i++) {
            List<String> row = rows.get(i);
            if (row.size() != longest.size()) {
                System.out.println("ERROR: row is a different length than the header");
                return;
            }
            for (int j = 0; j < row.size(); j++) {
                if (row.get(i).length() > longest.get(i))
                    longest.set(i, row.get(i).length());
            }
        }
        prettyPrint(header, rows, longest, out);
    }

    public boolean handleSpecialCommands(String input, PrintStream out) {
        input = input.substring(0, input.length()-1);
        String[] pieces = input.split("\\s+");
        String cmd = pieces[0];
        if ((cmd.equalsIgnoreCase("desc")) || (cmd.equalsIgnoreCase("describe"))) {
            return handleDescribe(input, pieces, out);
        }
        return false;
    }

    public boolean handleDescribe(String input, String[] pieces, PrintStream out) {
        if (pieces.length < 2) {
            System.out.println("ERROR: bad describe: " + input);
            return true;
        }
        if (pieces[1].equalsIgnoreCase("keyspaces")) {
            describeKeyspaces(out);
            return true;
        }
        if (pieces[1].equalsIgnoreCase("tables")) {
            if (pieces.length > 2) {
                describeTables(pieces[2], out);
                return true;
            }
            else {
                if (null != session.getLoggedKeyspace()) {
                    describeTables(session.getLoggedKeyspace(), out);
                    return true;
                }
                else {
                    System.out.println("ERROR: must specify keyspace to list tables");
                    return true;
                }
            }
        }
        if (pieces[1].equalsIgnoreCase("table")) {
            if (pieces.length < 3) {
                System.out.println("ERROR: must specify table");
                return true;
            }
            else {
                if (pieces.length > 3) {
                    describeTable(pieces[2], pieces[3], out);
                    return true;
                }
                else {
                    if (pieces[2].contains(".")) {
                        describeTable(pieces[2], out);
                        return true;
                    }
                    else {
                        String keyspace = session.getLoggedKeyspace();
                        if (null == keyspace) {
                            System.err.println("ERROR: bad describe table command: " + input);
                            return true;
                        }
                        describeTable(keyspace, pieces[2], out);
                        return true;
                    }
                }
            }
        }
        System.out.println("ERROR: bad describe command: " + input);
        return true;
    }

    public void describeKeyspaces(PrintStream out) {
        System.out.println(" ==> DESCRIBE KEYSPACES");
        session.getCluster()
                .getMetadata()
                .getKeyspaces()
                .forEach(km -> System.out.println(" " + km.getName()));
        System.out.println();
    }

    public void describeTables(String keyspace, PrintStream out) {
        if (null == session.getCluster().getMetadata().getKeyspace(keyspace))
            System.out.println("ERROR: keyspace (" + keyspace + ") not found");
        System.out.println(" ==> DESCRIBE TABLES");
        session.getCluster()
                .getMetadata()
                .getKeyspace(keyspace)
                .getTables()
                .forEach(tm -> System.out.println(" " + tm.getName()));
        System.out.println();
    }

    public void describeTable(String keyspace, String table, PrintStream out) {
        KeyspaceMetadata km = session.getCluster().getMetadata().getKeyspace(keyspace);
        if (null == km) {
            System.out.println("ERROR: keyspace (" + keyspace + ") not found");
            return;
        }
        TableMetadata tm = km.getTable(table);
        if (null == tm) {
            System.out.println("ERROR: table (" + keyspace + "." + table + ") not found");
            return;
        }
        System.out.println(" ==> DESCRIBE TABLE");
        System.out.println(tm.exportAsString());
        System.out.println();
    }

    public void describeTable(String keyspacetable, PrintStream out) {
        String[] pieces = keyspacetable.split(".");
        if (pieces.length != 2) {
            System.out.println("ERROR: poorly formatted table (" + keyspacetable + ")");
            return;
        }
        String keyspace = pieces[0];
        String table = pieces[1];
        describeTable(keyspace, table, out);
    }
}