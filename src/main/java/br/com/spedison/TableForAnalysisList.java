package br.com.spedison;

import java.util.LinkedList;

public class TableForAnalysisList extends LinkedList<TableForAnalysis> {

    public void addSrc(String schema, String tableName, String fieldIdName){
        TableForAnalysis f = new TableForAnalysis();
        f.setSchemaName(schema);
        f.setTableName(tableName);
        f.setFieldIdName(fieldIdName);
        f.setExistsSrc(true);
        f.setExistsDst(false);
        int pos = this.indexOf(f);

        if (pos >= 0 ){
            TableForAnalysis item = this.get(pos);
            item.setExistsSrc(true);
        } else {
            add(f);
        }
    }

    public void addDst(String schema, String tableName, String fieldIdName){
        TableForAnalysis f = new TableForAnalysis();
        f.setSchemaName(schema);
        f.setTableName(tableName);
        f.setFieldIdName(fieldIdName);
        f.setExistsDst(true);
        f.setExistsSrc(false);
        int pos = this.indexOf(f);

        if (pos >= 0 ){
            TableForAnalysis item = this.get(pos);
            item.setExistsDst(true);
        } else {
            add(f);
        }
    }

}
