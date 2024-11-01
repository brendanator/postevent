package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class PostgresDebeziumConnectorTest {

    @Test
    void testEngineInitialization() {
        Properties props = new Properties();
        props.setProperty("name", "test-connector");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        
        PostgresDebeziumConnector connector = new PostgresDebeziumConnector(props);
        assertNotNull(connector);
    }

    @Test
    void testConsumerRegistration() {
        Properties props = new Properties();
        AtomicBoolean called = new AtomicBoolean(false);
        
        PostgresDebeziumConnector connector = new PostgresDebeziumConnector(props);
        connector.setChangeEventConsumer(event -> called.set(true));
        
        assertNotNull(connector);
    }
}
