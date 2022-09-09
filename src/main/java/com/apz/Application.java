package com.apz;

import oracle.jdbc.OracleConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Application {

    private static final Logger logger = LogManager.getLogger(Application.class);

    public static void main(String[] args) throws ClassNotFoundException {

        for (int i = 0; i < args.length; i++) {
            logger.info("arg {} = {}", i, args[i]);
        }

        Class.forName("oracle.jdbc.driver.OracleDriver");

        if (args.length != 3) {
            logger.error("Invalid number of arguments: Must provide 3 arguments in the format: <schema_name> <schema_password> jdbc:oracle:thin:@//<host>:<port>/<SID>");
            return;
        }

        Properties properties = new Properties();
        properties.setProperty("user", args[0]);
        properties.setProperty("password", args[1]);
        properties.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_NET_CONNECT_TIMEOUT, "10000");

        try {
            logger.info("****** Starting JDBC Connection test *******");
            String sqlQuery = "select sysdate from dual";

            Connection conn = DriverManager.getConnection(args[2], properties);
            conn.setAutoCommit(false);
            Statement statement = conn.createStatement();
            logger.info("Running SQL query: [{}]", sqlQuery);
            ResultSet resultSet = statement.executeQuery(sqlQuery);

            while (resultSet.next()) {
                logger.info("Result of SQL query: [{}]", resultSet.getString(1));
            }

            statement.close();
            conn.close();

            logger.info("JDBC connection test successful!");
        } catch (SQLException ex) {
            logger.error("Exception occurred connecting to database: {}", ex.getMessage());
        }
    }
}