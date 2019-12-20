package hessian.cqlish;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.collect.Lists;
import de.vandermeer.asciitable.AT_Row;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestWord;
import de.vandermeer.asciithemes.TA_GridThemes;
import de.vandermeer.asciithemes.a8.A8_Grids;
import org.apache.cassandra.thrift.ColumnDef;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.cassandraunit.utils.RestartableEmbeddedCassandraServerHelper;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class CqlishApplication {
    private Session session;
    private CodecRegistry codecRegistry;
    private String scriptFile;
    private LineReader reader;
    private Terminal terminal;

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

    public boolean setup() throws Exception {
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

    public boolean doRepl() throws Exception {
        terminal = TerminalBuilder.builder()
                .system(true)
                .streams(System.in, System.out)
                .name("cqlish")
                .build();
        String firstPrompt = "cqlish> ";
        String continuedPrompt = "   ===> ";
        System.out.println(terminal.getName() + ": " + terminal.getType());

        DefaultHistory history = new DefaultHistory();
        reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .variable(LineReader.INDENTATION, 2)
                .history(history)
                .build();

        String cql = "";
        while (true) {
            String prompt = cql.isEmpty() ? firstPrompt : continuedPrompt;
            String line = reader.readLine(prompt, "", (MaskingCallback) null, null);
            line = line.trim();

            if (line.equalsIgnoreCase("quit")
                    || line.equalsIgnoreCase("quit;")
                    || line.equalsIgnoreCase("exit")
                    || line.equalsIgnoreCase("exit;"))
                break;

            if (line.endsWith("%%")) {
                System.out.println(" ... clearing buffer");
                cql = "";
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
        return true;
    }

    public void describeKeyspaces(PrintStream out) {
        System.out.println(" ==> DESCRIBE KEYSPACES");
        /*List<List<String>> keyspaces =*/ session.getCluster()
                .getMetadata()
                .getKeyspaces()
                .forEach(km -> System.out.println(" " + km.getName()));
        /*
                .stream()
                .map(km -> Lists.newArrayList(km.getName()))
                .collect(Collectors.toList());
                */
        System.out.println();
    }

    public void describeTables(String keyspace, PrintStream out) {
        if (null == session.getCluster().getMetadata().getKeyspace(keyspace))
            System.out.println("ERROR: keyspace (" + keyspace + ") not found");
        System.out.println(" ==> DESCRIBE TABLES");
        /*List<List<String>> tables =*/ session.getCluster()
                .getMetadata()
                .getKeyspace(keyspace)
                .getTables()
                .forEach(tm -> System.out.println(" " + tm.getName()));
                /*.stream()
                .map(tm -> Lists.newArrayList(tm.getName()))
                .collect(Collectors.toList());
        List<String> header = Lists.newArrayList(new String("Table"));
        prettyPrint(header, tables, out);
        */
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
        //System.out.println(tm.asCQLQuery());
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