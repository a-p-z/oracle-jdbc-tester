package com.apz;

import com.github.rvesse.airline.SingleCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Application {
    private static final Logger logger = LogManager.getLogger(Application.class);

    public static void main(final String[] args) {
        try {
            SingleCommand<JdbcConnectionTestCommand> parser = SingleCommand.singleCommand(JdbcConnectionTestCommand.class);
            JdbcConnectionTestCommand cmd = parser.parse(args);
            cmd.run();

        } catch (final Exception e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }
}