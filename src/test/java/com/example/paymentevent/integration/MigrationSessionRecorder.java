package com.example.paymentevent.integration;

import org.flywaydb.core.api.callback.Callback;
import org.flywaydb.core.api.callback.Context;
import org.flywaydb.core.api.callback.Event;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Flyway callback (Spring Boot hands every Callback bean to Flyway) that records the session
 * settings of the connection the migrations actually run on. Imported by
 * AbstractIntegrationTest so every IT shares one Spring context; Flyway runs once, when that
 * context starts, so the values are captured then and read by DatabaseTimeoutsIT.
 */
public class MigrationSessionRecorder implements Callback {

    private volatile String statementTimeout;
    private volatile String lockTimeout;

    @Override
    public boolean supports(Event event, Context context) {
        return event == Event.BEFORE_MIGRATE;
    }

    @Override
    public boolean canHandleInTransaction(Event event, Context context) {
        return true;
    }

    @Override
    public void handle(Event event, Context context) {
        try (Statement statement = context.getConnection().createStatement()) {
            statementTimeout = show(statement, "statement_timeout");
            lockTimeout = show(statement, "lock_timeout");
        } catch (SQLException e) {
            throw new IllegalStateException("Could not read migration session settings", e);
        }
    }

    @Override
    public String getCallbackName() {
        return "migration-session-recorder";
    }

    public String statementTimeout() {
        return statementTimeout;
    }

    public String lockTimeout() {
        return lockTimeout;
    }

    private static String show(Statement statement, String setting) throws SQLException {
        try (ResultSet rs = statement.executeQuery("SHOW " + setting)) {
            rs.next();
            return rs.getString(1);
        }
    }
}
