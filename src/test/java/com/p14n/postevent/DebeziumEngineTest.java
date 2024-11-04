package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumEngineTest {
    private DebeziumEngineWrapper engine;
    private Properties properties;

    @BeforeEach
    void setUp() {
        properties = new Properties();
        properties.setProperty("name", "test-connector");
        properties.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        properties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        properties.setProperty("offset.flush.interval.ms", "1000");
        
        engine = new DebeziumEngineWrapper(properties);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
    }

    @Test
    void testEngineInitialization() {
        assertNotNull(engine);
    }

    @Test
    void testStartWithoutConsumer() {
        assertThrows(IllegalStateException.class, () -> engine.start());
    }

    @Test
    void testSetConsumerAndStart() {
        AtomicBoolean consumerCalled = new AtomicBoolean(false);
        
        engine.setChangeEventConsumer((ChangeEvent<String, String> event) -> {
            consumerCalled.set(true);
        });
        
        engine.start();
        
        // Engine should start without throwing exceptions
        assertDoesNotThrow(() -> Thread.sleep(1000));
    }
}
