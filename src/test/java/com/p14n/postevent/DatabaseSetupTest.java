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
    void testCreateSchema() throws SQLException {
        databaseSetup.createSchemaIfNotExists();
        
        // Verify schema exists
        ResultSet schemas = connection.getMetaData().getSchemas();
        boolean found = false;
        while (schemas.next()) {
            if ("postevent".equals(schemas.getString("TABLE_SCHEM"))) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Schema 'postevent' should exist");
    }

    @Test
    void testCreateTable() throws SQLException {
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists("events");

        // Verify table exists
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, "postevent", "events", null);
        assertTrue(tables.next(), "Table 'events' should exist");

        // Verify columns
        ResultSet columns = metaData.getColumns(null, "postevent", "events", null);
        int columnCount = 0;
        while (columns.next()) {
            columnCount++;
        }
        assertEquals(9, columnCount, "Table should have 9 columns");
    }

    @Test
    void testIdempotentExecution() throws SQLException {
        // Execute twice to verify idempotency
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists("events");
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists("events");

        // Should not throw any exceptions
    }

    @Test
    void testInvalidTableName() {
        databaseSetup.createSchemaIfNotExists();
        
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists("invalid-table-name"));
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists("1invalid"));
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists(null));
    }

    @Test
    void testNullConnection() {
        assertThrows(IllegalArgumentException.class, () -> 
            new DatabaseSetup(null));
    }
}
