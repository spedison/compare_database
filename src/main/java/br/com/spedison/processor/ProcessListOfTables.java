package br.com.spedison.processor;

import br.com.spedison.helper.ConnectionHelper;
import br.com.spedison.helper.QueryHelper;
import br.com.spedison.vo.TableForAnalysis;
import br.com.spedison.vo.TableForAnalysisList;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProcessListOfTables {

    ConnectionHelper connectionHelperSrc;
    ConnectionHelper connectionHelperDst;
    TableForAnalysisList tableForAnalysisList;
    ExecutorService executorOfListTasks = null;


    public ProcessListOfTables(ConnectionHelper connectionHelperSrc, ConnectionHelper connectionHelperDst) {
        this.connectionHelperSrc = connectionHelperSrc;
        this.connectionHelperDst = connectionHelperDst;

        if (!connectionHelperDst.getDatabaseTypeName().equalsIgnoreCase(connectionHelperDst.getDatabaseTypeName()))
            throw new RuntimeException("Database types must have be equals");
    }

    public TableForAnalysisList makeListOfTableForAnalysis() throws SQLException {
        TableForAnalysisList ret = new TableForAnalysisList();
        QueryHelper qr = new QueryHelper(connectionHelperDst.getDatabaseTypeName(), "query_list_tables");
        String sql = qr.loadQuery();
        Connection connDst = connectionHelperDst.createNewConnection();
        Connection connSrc = connectionHelperSrc.createNewConnection();

        Statement psDst = connDst.createStatement();
        ResultSet rsDst = psDst.executeQuery(sql);
        if (Objects.nonNull(rsDst)) {
            while (rsDst.next()) {
                String schema = rsDst.getString("SCHEMA_NAME");
                String table = rsDst.getString("TABLE_NAME");
                String fieldId = rsDst.getString("KEY_FIELD_NAME");
                ret.addDst(schema, table, fieldId);
            }
        }

        Statement psSrc = connSrc.createStatement();
        ResultSet rsSrc = psSrc.executeQuery(sql);
        if (Objects.nonNull(rsSrc)) {
            while (rsSrc.next()) {
                String schema = rsSrc.getString("SCHEMA_NAME");
                String table = rsSrc.getString("TABLE_NAME");
                String fieldId = rsSrc.getString("KEY_FIELD_NAME");
                ret.addSrc(schema, table, fieldId);
            }
        }

        tableForAnalysisList = ret;
        return ret;
    }


    public void processCountRegisterComparation(int numberOfThreads) {

        List<ProcessConterRegisters> listToProcess = new ArrayList<>();

        tableForAnalysisList
                .stream()
                .map(tfa -> new ProcessConterRegisters(tfa, connectionHelperSrc, connectionHelperDst))
                .forEach(listToProcess::add);

        executorOfListTasks = Executors.newFixedThreadPool(numberOfThreads);
        listToProcess.stream().forEach(executorOfListTasks::execute);
    }

    public Long getTablesDone() {
        return tableForAnalysisList.stream().filter(TableForAnalysis::getExecuted).count();
    }

    public boolean taskCountProgressTerminate(boolean showMessage) {
        boolean terminate = getTablesDone() ==
                tableForAnalysisList.size();

        if (showMessage) {
            System.out.println(Instant.now() + " - NumberOf Tasks %d of %d".formatted(
                    tableForAnalysisList.stream().filter(TableForAnalysis::getExecuted).count(),
                    tableForAnalysisList.size()
            ));
        }

        if (terminate) {
            executorOfListTasks.shutdown();
            try {
                executorOfListTasks.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                System.err.println(Instant.now() + " - Problemas while terminate pool of threads");
            }
        }
        return terminate;
    }
}
