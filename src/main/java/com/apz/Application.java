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
    private static final String SELECT_SYSDATE_FROM_DUAL = "select sysdate from dual";

    public static void main(String[] args) {

        for (int i = 0; i < args.length; i++) {
            logger.info("arg {} = {}", i, args[i]);
        }

        if (args.length != 3) {
            logger.error("Invalid number of arguments: Must provide 3 arguments in the format: <schema_name> <schema_password> jdbc:oracle:thin:@//<host>:<port>/<SID>");
            return;
        }

        final OracleDataSource oracleDataSource = oracleDataSource(args[0], args[1], args[2], 10000);

        makeConnectionAttempt(oracleDataSource);
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

    private static void makeConnectionAttempt(final OracleDataSource oracleDataSource) {
        try (final Connection connection = oracleDataSource.getConnection();
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery(SELECT_SYSDATE_FROM_DUAL)) {

            logger.debug("Running SQL query: [{}]", SELECT_SYSDATE_FROM_DUAL);

            while (resultSet.next()) {
                logger.debug("Result of SQL query: [{}]", resultSet.getString(1));
            }

            logger.info("JDBC connection test successful!");

        } catch (final SQLException e) {
            logger.warn("Exception occurred connecting to {}: {}", "oracleDataSource.getURL()", e.getMessage());
        }
    }
}