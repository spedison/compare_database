package br.com.spedison;

import br.com.spedison.helper.ConnectionHelper;
import br.com.spedison.processor.ProcessListOfTables;
import br.com.spedison.vo.TableForAnalysisList;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

/***
 * This class is entry point to execute comparation.
 * The comparation is:
 *  - Number of registers
 *  - First id of register
 *  - Last id of register
 *  TODO: Compare the sequences
 *    - Number
 *    - Values
 */
public class Main {

    ConnectionHelper connectionHelperSrc;
    ConnectionHelper connectionHelperDst;
    Integer numberOfThreads;
    TableForAnalysisList tablesForAnalysis;
    ProcessListOfTables processListOfTables;
    public void main(String[] args) {
        try {
            new Main().execute(args);
        } catch (SQLException seq) {
            System.err.println(Instant.now() + " - Problems with connection database." + seq.getMessage());
        } catch (IOException ioe) {
            System.err.println(Instant.now() + " - Problems while read/wite file. " + ioe.getMessage());
        }
    }

    private void execute(String[] args) throws IOException, SQLException {
        extractDataFromParams(args);

        initProcess();

        processListOfTables.processCountRegisterComparation(numberOfThreads);

        waitProcess();

        printReport();
    }

    private void printReport() {
        //TODO: Make it better.
        tablesForAnalysis.forEach(System.out::println);
    }

    private void initProcess() throws SQLException {
        processListOfTables = new ProcessListOfTables(connectionHelperSrc, connectionHelperDst);
        tablesForAnalysis = processListOfTables.makeListOfTableForAnalysis();
    }

    private void extractDataFromParams(String[] args) throws IOException {
        connectionHelperSrc = ConnectionHelper.fromFileProperties(args[0]);
        connectionHelperDst = ConnectionHelper.fromFileProperties(args[1]);
        numberOfThreads =
                args.length == 3 ?
                        Integer.parseInt(args[2]) : 5;
    }

    private void waitProcess() {
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
    }
}