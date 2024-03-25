package br.com.spedison;

import br.com.spedison.helper.ConnectionHelper;
import br.com.spedison.helper.QueryHelper;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {

        ConnectionHelper connectionHelperSrc = ConnectionHelper.fromFileProperties(args[0]);
        ConnectionHelper connectionHelperDst = ConnectionHelper.fromFileProperties(args[1]);
        Integer numberOfThreads =
                args.length == 3 ?
                        Integer.parseInt(args[2]) : 5;

        TableForAnalysisList tablesForAnalysis;

        ProcessListOfTables processListOfTables = new ProcessListOfTables(connectionHelperSrc, connectionHelperDst);
        tablesForAnalysis = processListOfTables.makeListOfTableForAnalysis();

        processListOfTables.processCountRegisterComparation(numberOfThreads);

        long lastCheck = 0;
        while (!processListOfTables.taskCountProgressTerminate(lastCheck != processListOfTables.getTablesDone())) {
            lastCheck = processListOfTables.getTablesDone();
            try {
                Thread.yield();
                Thread.sleep(10);
            } catch (InterruptedException ie) {
                break;
            }
        }

        tablesForAnalysis.forEach(System.out::println);

    }
}