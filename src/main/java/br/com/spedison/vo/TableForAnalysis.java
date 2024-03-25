package br.com.spedison.vo;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class TableForAnalysis {

    String schemaName;
    String tableName;
    String fieldIdName;
    boolean existsSrc;
    boolean existsDst;
    long numberOfRegistersSrc;
    long numberOfRegistersDst;

    long lastIdSrc;
    long lastIdDst;

    long firstIdSrc;
    long firsIdDst;

    AtomicBoolean executed = new AtomicBoolean(false);

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFieldIdName() {
        return fieldIdName;
    }

    public void setFieldIdName(String fieldIdName) {
        this.fieldIdName = fieldIdName;
    }

    public boolean isExistsSrc() {
        return existsSrc;
    }

    public String getFullName() {
        return "%s.%s".formatted(getSchemaName(), getTableName());
    }

    public void setExistsSrc(boolean existsSrc) {
        this.existsSrc = existsSrc;
    }

    public boolean isExistsDst() {
        return existsDst;
    }

    public void setExistsDst(boolean existsDst) {
        this.existsDst = existsDst;
    }

    public long getNumberOfRegistersSrc() {
        return numberOfRegistersSrc;
    }

    public void setNumberOfRegistersSrc(long numberOfRegistersSrc) {
        this.numberOfRegistersSrc = numberOfRegistersSrc;
    }

    public long getNumberOfRegistersDst() {
        return numberOfRegistersDst;
    }

    public void setNumberOfRegistersDst(long numberOfRegistersDst) {
        this.numberOfRegistersDst = numberOfRegistersDst;
    }

    public long getLastIdSrc() {
        return lastIdSrc;
    }

    public void setLastIdSrc(long lastIdSrc) {
        this.lastIdSrc = lastIdSrc;
    }

    public long getLastIdDst() {
        return lastIdDst;
    }

    public void setLastIdDst(long lastIdDst) {
        this.lastIdDst = lastIdDst;
    }

    public long getFirstIdSrc() {
        return firstIdSrc;
    }

    public void setFirstIdSrc(long firstIdSrc) {
        this.firstIdSrc = firstIdSrc;
    }

    public long getFirsIdDst() {
        return firsIdDst;
    }

    public void setFirsIdDst(long firsIdDst) {
        this.firsIdDst = firsIdDst;
    }

    public boolean getExecuted() {
        return executed.get();
    }

    public void setExecuted() {
        this.executed.set(true);
    }

    public String getStatus() {

        if (existsSrc ^ existsDst)
            return "Exists only " + (existsDst ? "DST" : "SRC") + " table.";

        if (numberOfRegistersSrc == numberOfRegistersDst &&
                Objects.nonNull(getFieldIdName()) &&
                lastIdDst == lastIdSrc &&
                firsIdDst == firstIdSrc) return "EQUALS";

        if (numberOfRegistersSrc == numberOfRegistersDst &&
                Objects.nonNull(getFieldIdName()))
            return "ONLY NUMBER OF REGISTERS IS EQUALS - With id diferents";

        if (numberOfRegistersSrc == numberOfRegistersDst &&
                Objects.isNull(getFieldIdName()))
            return "NUMBER OF REGISTERS IS EQUALS - With no check id";

        return "NUMBER OF REGISTERS IS DIFERENTS";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableForAnalysis tableForAnalysis)) return false;
        if (!getSchemaName().equals(tableForAnalysis.getSchemaName())) return false;
        if (!getTableName().equals(tableForAnalysis.getTableName())) return false;
        if (Objects.nonNull(getFieldIdName()) ^ Objects.nonNull(tableForAnalysis.getFieldIdName())) return false;
        if (Objects.nonNull(getFieldIdName()) && Objects.nonNull(tableForAnalysis.getFieldIdName()))
            return getFieldIdName().equalsIgnoreCase(tableForAnalysis.getFieldIdName());
        return true;
    }

    @Override
    public int hashCode() {
        int result = getSchemaName().hashCode();
        result = 31 * result + getTableName().hashCode();
        if (Objects.nonNull(getFieldIdName())) {
            result += (310 * getFieldIdName().hashCode());
        }
        return result;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("TableForAnalysis{");
        sb.append("Status = '").append(getStatus()).append("'");
        sb.append(", schema='").append(schemaName).append("'");
        sb.append(", name='").append(tableName).append("'");
        sb.append(", nameField='").append(Objects.requireNonNullElse(fieldIdName, "<Empty>"));
        sb.append('\'');
        sb.append(", existsSrc=").append(existsSrc);
        sb.append(", existsDst=").append(existsDst);
        sb.append(", numberOfRegistersSrc=").append(numberOfRegistersSrc);
        sb.append(", numberOfRegistersDst=").append(numberOfRegistersDst);
        sb.append(", lastIdSrc=").append(lastIdSrc);
        sb.append(", lastIdDst=").append(lastIdDst);
        sb.append(", firstIdSrc=").append(firstIdSrc);
        sb.append(", firsIdDst=").append(firsIdDst);
        sb.append('}');
        return sb.toString();
    }
}
