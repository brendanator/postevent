package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumEngineTest {

    @Test
    void engineInitializesWithProperties() {
        Properties props = new Properties();
        props.setProperty("name", "test-connector");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        
        DebeziumEngine engine = new DebeziumEngine(props);
        assertNotNull(engine);
    }

    @Test
    void engineAcceptsConsumerFunction() {
        Properties props = new Properties();
        props.setProperty("name", "test-connector");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        
        DebeziumEngine engine = new DebeziumEngine(props);
        AtomicBoolean consumerCalled = new AtomicBoolean(false);
        
        engine.setChangeEventConsumer((ChangeEvent<String, String> event) -> {
            consumerCalled.set(true);
        });
        
        assertDoesNotThrow(() -> engine.start());
    }

    @Test
    void engineFailsToStartWithoutConsumer() {
        Properties props = new Properties();
        props.setProperty("name", "test-connector");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        
        DebeziumEngine engine = new DebeziumEngine(props);
        
        assertThrows(IllegalStateException.class, () -> engine.start());
    }
}
