package com.p14n.postevent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class DatabaseSetupTest {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14-alpine");

    private DatabaseSetup databaseSetup;

    @BeforeEach
    void setUp() {
        databaseSetup = new DatabaseSetup(
            postgres.getJdbcUrl(),
            postgres.getUsername(),
            postgres.getPassword()
        );
    }

    @Test
    void shouldCreateSchemaIfNotExists() throws SQLException {
        databaseSetup.createSchemaIfNotExists();

        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword())) {
            
            ResultSet schemas = conn.getMetaData().getSchemas();
            boolean schemaExists = false;
            while (schemas.next()) {
                if ("postevent".equals(schemas.getString("TABLE_SCHEM"))) {
                    schemaExists = true;
                    break;
                }
            }
            assertTrue(schemaExists, "Schema 'postevent' should exist");
        }
    }

    @Test
    void shouldCreateTableIfNotExists() throws SQLException {
        String topic = "test_topic";
        databaseSetup.createSchemaIfNotExists();
        databaseSetup.createTableIfNotExists(topic);

        try (Connection conn = DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword())) {
            
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, "postevent", topic, new String[]{"TABLE"});
            
            assertTrue(tables.next(), "Table should exist");
            assertEquals(topic, tables.getString("TABLE_NAME"));
            
            // Verify columns
            ResultSet columns = metaData.getColumns(null, "postevent", topic, null);
            int columnCount = 0;
            while (columns.next()) {
                columnCount++;
                String columnName = columns.getString("COLUMN_NAME");
                switch (columnName) {
                    case "idn":
                        assertEquals("bigint", columns.getString("TYPE_NAME").toLowerCase());
                        break;
                    case "id":
                    case "source":
                    case "type":
                        assertEquals("varchar", columns.getString("TYPE_NAME").toLowerCase());
                        assertEquals("NO", columns.getString("IS_NULLABLE"));
                        break;
                }
            }
            assertEquals(9, columnCount, "Table should have 9 columns");
        }
    }

    @Test
    void shouldHandleIdempotentOperations() {
        String topic = "test_topic";
        
        // Execute operations twice
        assertDoesNotThrow(() -> {
            databaseSetup.createSchemaIfNotExists();
            databaseSetup.createTableIfNotExists(topic);
            databaseSetup.createSchemaIfNotExists();
            databaseSetup.createTableIfNotExists(topic);
        });
    }

    @Test
    void shouldRejectInvalidTopicName() {
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists(null));
        
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists(""));
        
        assertThrows(IllegalArgumentException.class, () -> 
            databaseSetup.createTableIfNotExists("   "));
    }
}
