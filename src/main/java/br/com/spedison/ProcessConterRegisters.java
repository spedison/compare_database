package br.com.spedison;

import br.com.spedison.helper.ConnectionHelper;
import br.com.spedison.helper.QueryHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public class ProcessConterRegisters implements Runnable {

    TableForAnalysis itemForAnalysis;
    ConnectionHelper connectionHelperSrc;
    ConnectionHelper connectionHelperDst;

    public ProcessConterRegisters(TableForAnalysis itemForAnalysis, ConnectionHelper connectionHelperSrc, ConnectionHelper connectionHelperDst) {
        this.itemForAnalysis = itemForAnalysis;
        this.connectionHelperSrc = connectionHelperSrc;
        this.connectionHelperDst = connectionHelperDst;
    }

    @Override
    public void run() {

        Connection connSrc = connectionHelperSrc.createNewConnection();
        Connection connDst = connectionHelperDst.createNewConnection();

        if (Objects.isNull(connDst) || Objects.isNull(connSrc)) {
            System.err.println("Problems while open connection with database");
            itemForAnalysis.setExecuted();
            return;
        }

        boolean complete = Objects.nonNull(itemForAnalysis.getFieldIdName());

        QueryHelper queryHelper = new QueryHelper(connectionHelperDst.getDatabaseTypeName(), "query_count_" + (
                complete ? "complete" : "simple"
        ));

        String sql =
                complete ?
                        queryHelper.loadQuery().formatted(itemForAnalysis.getFieldIdName(), itemForAnalysis.getFieldIdName(), itemForAnalysis.getFullName()) :
                        queryHelper.loadQuery().formatted(itemForAnalysis.getFullName());

        try {
            Statement stSrc = connSrc.createStatement();
            ResultSet rsSrc = stSrc.executeQuery(sql);
            if (Objects.nonNull(rsSrc) && rsSrc.next()) {
                itemForAnalysis.numberOfRegistersSrc = rsSrc.getLong(1);
                if (complete) {
                    itemForAnalysis.lastIdSrc = rsSrc.getLong(2);
                    itemForAnalysis.firstIdSrc = rsSrc.getLong(3);
                }
            }
            connSrc.close();
            connSrc = null;
        } catch (SQLException sqlE) {
            if (Objects.nonNull(connSrc))
                try {
                    connSrc.close();
                } catch (SQLException sqle) {
                }
            itemForAnalysis.numberOfRegistersSrc = -1;
        }

        try {
            Statement stDst = connDst.createStatement();
            ResultSet rsDst = stDst.executeQuery(sql);
            if (Objects.nonNull(rsDst) && rsDst.next()) {
                itemForAnalysis.numberOfRegistersDst = rsDst.getLong(1);
                if (complete) {
                    itemForAnalysis.lastIdDst = rsDst.getLong(2);
                    itemForAnalysis.firsIdDst = rsDst.getLong(3);
                }
            }
            connDst.close();
            connDst = null;
        } catch (SQLException sqlE) {
            if (Objects.nonNull(connDst))
                try {
                    connDst.close();
                } catch (SQLException sqle) {
                }
            itemForAnalysis.numberOfRegistersDst = -1;
        }
        itemForAnalysis.setExecuted();
    }
}