package com.apz;

import com.dyngr.Polling;
import com.dyngr.exception.PollerStoppedException;
import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import com.github.rvesse.airline.annotations.restrictions.RequireSome;
import com.github.rvesse.airline.annotations.restrictions.RequiredOnlyIf;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static java.lang.Integer.MAX_VALUE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_THIN_NET_CONNECT_TIMEOUT;

@Command(name = "oracle-jdbc-test", description = "A simple command line application to test JDBC connection to Oracle Database.")
public class JdbcConnectionTestCommand implements Runnable {

    private static final Logger logger = LogManager.getLogger(JdbcConnectionTestCommand.class);

    @Inject
    public HelpOption<JdbcConnectionTestCommand> helpOption;

    @Option(name = {"--url"},
            description = "URL of the database connection string..",
            title = "url")
    @RequireOnlyOne(tag = "url/params")
    protected String url;

    @Option(name = {"--username", "--user", "-u"},
            description = "The user name when connecting to the database.",
            title = "user")
    protected String user;

    @Option(name = {"--password", "-p"},
            description = "Password for the connecting user.",
            title = "password")
    protected String password;

    @Option(name = "--transport-connect-timeout",
            description = "The connect timeout controls how much time is allowed to connect the socket to the database. Successfully connecting the socket doesn't necessarily mean that the database service is up but it means that the listener is accepting connections.\n" +
                    "This value is in seconds.\n" +
                    "Default value is \"0\" (no timeout).",
            title = "timeout")
    protected int transportConnectTimeout = 0;

    @Option(name = {"--hostname", "--host", "--server-name", "--server"},
            description = "Name of the database server.",
            title = "server name")
    @RequireOnlyOne(tag = "url/params")
    protected String serverName = "";

    @RequiredOnlyIf(names = {"--hostname", "--host", "--server-name", "--server"})
    @Option(name = {"--port-number", "--port"},
            description = "Number of the port where the server listens for requests.",
            title = "port number")
    protected Integer portNumber;

    @Option(name = {"--service-name", "--service"},
            description = "Specifies the database service name for this data source.",
            title = "service name")
    protected String serviceName;

    @Option(name = {"--database-name", "--sid"},
            description = "Name of the particular database on the server. Also known as the SID in Oracle terminology.",
            title = "database name")
    protected String databaseName;

    @Option(name = "--wait-periodly",
            description = "Waits with a fixed interval in seconds.",
            title = "interval")
    protected int waitPeriodly = 10;

    @Option(name = "--stop-after-attempt",
            description = "Stops after N failed attempts.",
            title = "number of attempts")
    @RequireSome(tag = "stop-after")
    protected int stopAfterAttempt = MAX_VALUE;

    @Option(name = "--stop-after-delay",
            description = "Stops after a given delay in seconds.",
            title = "timeout")
    @RequireSome(tag = "stop-after")
    protected int stopAfterDelay = MAX_VALUE;

    public void run() {
        if (helpOption.showHelpIfRequested()) {
            return;
        }

        final OracleDataSource oracleDataSource = oracleDataSource();
        final JdbcConnectionAttemptMaker jdbcConnectionAttemptMaker = new JdbcConnectionAttemptMaker(oracleDataSource);

        try {
            Polling.waitPeriodly(waitPeriodly, SECONDS)
                    .stopAfterAttempt(stopAfterAttempt)
                    .stopAfterDelay(stopAfterDelay, SECONDS)
                    .run(jdbcConnectionAttemptMaker);
            logger.info("JDBC connection test successful!");

        } catch (final PollerStoppedException e) {
            logger.error("JDBC connection test failed!");
            System.exit(1);
        }
    }

    private OracleDataSource oracleDataSource() {
        final Properties properties = new Properties();
        properties.setProperty(CONNECTION_PROPERTY_THIN_NET_CONNECT_TIMEOUT, Integer.toString(transportConnectTimeout));

        try {
            final OracleDataSource oracleDataSource = new OracleDataSource();
            oracleDataSource.setConnectionProperties(properties);
            oracleDataSource.setDriverType("thin");
            Optional.ofNullable(databaseName).ifPresent(dn -> oracleDataSource.setDatabaseName(databaseName));
            Optional.ofNullable(password).ifPresent(p -> oracleDataSource.setPassword(password));
            Optional.ofNullable(portNumber).ifPresent(pn -> oracleDataSource.setPortNumber(portNumber));
            Optional.ofNullable(serverName).ifPresent(sn -> oracleDataSource.setServerName(serverName));
            Optional.ofNullable(serviceName).ifPresent(sn -> oracleDataSource.setServiceName(serviceName));
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
