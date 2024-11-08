package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class PostgresDebeziumConnectorTest {
    private PostgresDebeziumConnector connector;
    private Properties properties;

    @BeforeEach
    void setUp() {
        properties = new Properties();
        properties.setProperty("name", "test-postgres-connector");
        properties.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        properties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        properties.setProperty("offset.flush.interval.ms", "1000");
        
        connector = new PostgresDebeziumConnector(properties);
    }

    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.close();
        }
    }

    @Test
    void engineCreatedSuccessfully() {
        assertNotNull(connector);
    }

    @Test
    void throwsExceptionWhenStartedWithoutConsumer() {
        assertThrows(IllegalStateException.class, () -> connector.start());
    }

    @Test
    void engineStartsWithConsumer() {
        AtomicBoolean consumerCalled = new AtomicBoolean(false);
        
        connector.setChangeEventConsumer((ChangeEvent<String, String> event) -> {
            consumerCalled.set(true);
        });

        connector.start();
        
        // Note: In a real test, you would need to actually generate some changes
        // in the database to verify the consumer is called
    }
}
