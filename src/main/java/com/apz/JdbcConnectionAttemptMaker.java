package com.apz;

import com.dyngr.core.AttemptMaker;
import com.dyngr.core.AttemptResult;
import com.dyngr.core.AttemptResults;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcConnectionAttemptMaker implements AttemptMaker<Void> {
    private static final Logger logger = LogManager.getLogger(JdbcConnectionAttemptMaker.class);
    private static final String SELECT_SYSDATE_FROM_DUAL = "select sysdate from dual";

    private final OracleDataSource oracleDataSource;

    public JdbcConnectionAttemptMaker(final OracleDataSource oracleDataSource) {
        this.oracleDataSource = oracleDataSource;
    }

    @Override
    public AttemptResult<Void> process() {
        logger.debug("Running SQL query: [{}]", SELECT_SYSDATE_FROM_DUAL);

        try (final Connection connection = oracleDataSource.getConnection();
             final Statement statement = connection.createStatement();
             final ResultSet resultSet = statement.executeQuery(SELECT_SYSDATE_FROM_DUAL)) {

            while (resultSet.next()) {
                logger.debug("Result of SQL query: [{}]", resultSet.getString(1));
            }

            return AttemptResults.justFinish();

        } catch (final SQLException e) {
            logger.warn("Exception occurred connecting to {}: {}", "oracleDataSource.getURL()", e.getMessage());
            return AttemptResults.justContinue();
        }
    }
}
