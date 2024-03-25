package br.com.spedison.helper;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public record ConnectionHelper(String databaseType, String databaseDriver, String host, Integer port, String user,
                               String passwd, String databaseName, String othersParams) {

    static public ConnectionHelper fromFileProperties(String fileName) throws IOException {
        Properties prop = new Properties();
        prop.load(new FileReader(fileName));
        return fromProperties(prop);
    }

    static public ConnectionHelper fromProperties(Properties properties) {
        return new ConnectionHelper(
                properties.getProperty("type"),
                properties.getProperty("driver"),
                properties.getProperty("host"),
                Integer.parseInt(Objects.requireNonNullElse(properties.getProperty("port"), "5432")),
                properties.getProperty("user"),
                properties.getProperty("password"),
                properties.getProperty("databasename"),
                properties.getProperty("otherparams"));
    }

    public String getDatabaseTypeName() {
        if (Objects.isNull(databaseType) || databaseType.equalsIgnoreCase("postgres") || databaseType.equalsIgnoreCase("postgresql"))
            return "postgresql";
        return databaseType;
    }

    public Connection createNewConnection() {

        String formatedJDBCConnection;

        if (Objects.isNull(databaseType) || databaseType.equalsIgnoreCase("postgres") || databaseType.equalsIgnoreCase("postgresql")) {

            String driverNameLocal = Objects.requireNonNullElse(databaseDriver, "org.postgresql.Driver");
            Class dbDriver;
            try {
                // Load Driver
                dbDriver = Class.forName(driverNameLocal);
            } catch (ClassNotFoundException cnf) {
                System.err.println("Driver %s not load".formatted(driverNameLocal));
                return null;
            }

            formatedJDBCConnection = "jdbc:postgresql://%s:%d/%s?user=%s&password=%s".formatted(
                    host, Objects.requireNonNullElse(port, 5432), databaseName, user, passwd);
            if (Objects.nonNull(othersParams))
                formatedJDBCConnection = formatedJDBCConnection + "&" + othersParams;

            try {
                return DriverManager.getConnection(formatedJDBCConnection);
            } catch (SQLException e) {
                System.err.println("Error while open connection :: %s".formatted(formatedJDBCConnection));
                return null;
            }
        } else {  // I Will do it for others database.
            return null;
        }
    }

}
