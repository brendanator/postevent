package com.p14n.postevent;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseSetupTest {
    private EmbeddedPostgres pg;
    private Connection connection;
    private DatabaseSetup databaseSetup;

    @BeforeEach
    void setUp() throws IOException {
        pg = EmbeddedPostgres.start();
        connection = pg.getPostgresDatabase().getConnection();
        databaseSetup = new DatabaseSetup(connection);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Ignore
            }
        }
        if (pg != null) {
            pg.close();
        }
    }

    @Test
    void shouldCreateSchemaIfNotExists() throws SQLException {
        databaseSetup.createSchemaIfNotExists();
        
        // Verify schema exists
        try (ResultSet rs = connection.getMetaData().getSchemas()) {
            boolean found = false;
            while (rs.next()) {
                if ("postevent".equals(rs.getString("TABLE_SCHEM"))) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Schema 'postevent' should exist");
        }
    }

    @Test
    void shouldCreateTableIfNotExists() throws SQLException {
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists("events");

        // Verify table exists
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet rs = metaData.getTables(null, "postevent", "events", null)) {
            assertTrue(rs.next(), "Table 'events' should exist");
        }

        // Verify columns
        try (ResultSet rs = metaData.getColumns(null, "postevent", "events", null)) {
            assertTrue(rs.next());
            assertEquals("idn", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("id", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("source", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("type", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("datacontenttype", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("dataschema", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("subject", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("data", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("time", rs.getString("COLUMN_NAME"));
        }
    }

    @Test
    void shouldRejectInvalidTableNames() {
        assertThrows(IllegalArgumentException.class, () -> databaseSetup.createTableIfNotExists("1invalid"));
        assertThrows(IllegalArgumentException.class, () -> databaseSetup.createTableIfNotExists("invalid-table"));
        assertThrows(IllegalArgumentException.class, () -> databaseSetup.createTableIfNotExists(""));
        assertThrows(IllegalArgumentException.class, () -> databaseSetup.createTableIfNotExists(null));
    }

    @Test
    void shouldBeIdempotent() throws SQLException {
        // Should not throw exceptions when called multiple times
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists("events");
        databaseSetup.createTableIfNotExists("events");
    }
}
