package com.p14n.postevent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

public class DatabaseSetup {
    private final Connection connection;
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");

    public DatabaseSetup(Connection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("Connection cannot be null");
        }
        this.connection = connection;
    }

    public void createSchemaIfNotExists() throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(
                "CREATE SCHEMA IF NOT EXISTS postevent")) {
            stmt.execute();
        }
    }

    public void createTableIfNotExists(String tableName) throws SQLException {
        if (!isValidTableName(tableName)) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }

        String createTableSQL = String.format("""
            CREATE TABLE IF NOT EXISTS postevent.%s (
                idn bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                id VARCHAR(255) NOT NULL,
                source VARCHAR(1024) NOT NULL,
                type VARCHAR(255) NOT NULL,
                datacontenttype VARCHAR(255),
                dataschema VARCHAR(255),
                subject VARCHAR(255),
                data bytea,
                time TIMESTAMP WITH TIME ZONE default current_timestamp,
                UNIQUE (id, source)
            )""", tableName);

        try (PreparedStatement stmt = connection.prepareStatement(createTableSQL)) {
            stmt.execute();
        }
    }

    private boolean isValidTableName(String tableName) {
        return tableName != null && TABLE_NAME_PATTERN.matcher(tableName).matches();
    }
}
