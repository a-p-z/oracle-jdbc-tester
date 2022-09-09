package com.apz;

import oracle.jdbc.pool.OracleDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;

import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_THIN_NET_CONNECT_TIMEOUT;

public class Application {

    private static final Logger logger = LogManager.getLogger(Application.class);

    public static void main(String[] args) {

        for (int i = 0; i < args.length; i++) {
            logger.info("arg {} = {}", i, args[i]);
        }

        if (args.length != 3) {
            logger.error("Invalid number of arguments: Must provide 3 arguments in the format: <schema_name> <schema_password> jdbc:oracle:thin:@//<host>:<port>/<SID>");
            return;
        }
        final OracleDataSource oracleDataSource = oracleDataSource(args[0], args[1], args[2], 10000);

        try {
            logger.info("****** Starting JDBC Connection test *******");
            String sqlQuery = "select sysdate from dual";

            Connection conn = oracleDataSource.getConnection();
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

    private static OracleDataSource oracleDataSource(final String user, final String password, final String url, final int transportConnectTimeout) {
        final Properties properties = new Properties();
        properties.setProperty(CONNECTION_PROPERTY_THIN_NET_CONNECT_TIMEOUT, Integer.toString(transportConnectTimeout));

        try {
            final OracleDataSource oracleDataSource = new OracleDataSource();
            oracleDataSource.setConnectionProperties(properties);
            oracleDataSource.setDriverType("thin");
            Optional.ofNullable(password).ifPresent(p -> oracleDataSource.setPassword(password));
            Optional.ofNullable(url).ifPresent(u -> oracleDataSource.setURL(url));
            Optional.ofNullable(user).ifPresent(u -> oracleDataSource.setUser(user));
            return oracleDataSource;

        } catch (final SQLException e) {
            logger.error("Error creating oracle data source: {}", e.getMessage());
            System.exit(1);
            return null;
        }
    }
}