package com.p14n.postevent;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumEngineTest {

    @Test
    void testEngineInitialization() {
        Properties props = new Properties();
        props.setProperty("name", "test-engine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        
        DebeziumEngineWrapper engine = new DebeziumEngineWrapper(props);
        assertNotNull(engine);
    }

    @Test
    void testConsumerRegistration() {
        Properties props = new Properties();
        AtomicBoolean consumerCalled = new AtomicBoolean(false);
        
        DebeziumEngineWrapper engine = new DebeziumEngineWrapper(props);
        engine.setRecordConsumer(record -> consumerCalled.set(true));
        
        assertDoesNotThrow(() -> engine.start());
    }
}
package com.p14n.postevent;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumEngineTest {

    @Test
    void testEngineInitialization() {
        Properties props = new Properties();
        props.setProperty("name", "test-engine");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        
        DebeziumEngineWrapper engine = new DebeziumEngineWrapper(props);
        assertNotNull(engine);
    }

    @Test
    void testConsumerRegistration() {
        Properties props = new Properties();
        AtomicBoolean called = new AtomicBoolean(false);
        
        DebeziumEngineWrapper engine = new DebeziumEngineWrapper(props);
        engine.setRecordConsumer(record -> called.set(true));
        
        assertDoesNotThrow(() -> engine.start());
    }
}
package com.p14n.postevent;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumEngineTest {

    @Test
    void shouldInitializeWithProperties() {
        Properties props = new Properties();
        props.setProperty("name", "test-connector");
        
        DebeziumEngineWrapper engine = new DebeziumEngineWrapper(props);
        assertNotNull(engine);
    }

    @Test
    void shouldThrowExceptionWhenStartingWithoutConsumer() {
        Properties props = new Properties();
        props.setProperty("name", "test-connector");
        
        DebeziumEngineWrapper engine = new DebeziumEngineWrapper(props);
        assertThrows(IllegalStateException.class, engine::start);
    }

    @Test
    void shouldAcceptConsumerFunction() {
        Properties props = new Properties();
        props.setProperty("name", "test-connector");
        
        DebeziumEngineWrapper engine = new DebeziumEngineWrapper(props);
        AtomicBoolean called = new AtomicBoolean(false);
        
        engine.setRecordConsumer((SourceRecord record) -> called.set(true));
        assertDoesNotThrow(engine::start);
    }
}
package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumEngineTest {

    @Test
    void shouldInitializeWithProperties() {
        Properties props = new Properties();
        props.setProperty("name", "test-connector");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        
        DebeziumEngine engine = new DebeziumEngine(props);
        assertNotNull(engine);
    }

    @Test
    void shouldAcceptChangeEventConsumer() {
        Properties props = new Properties();
        AtomicBoolean consumerCalled = new AtomicBoolean(false);
        
        DebeziumEngine engine = new DebeziumEngine(props);
        engine.setChangeEventConsumer(event -> consumerCalled.set(true));
        
        assertDoesNotThrow(() -> engine.start());
    }

    @Test
    void shouldThrowExceptionIfStartedWithoutConsumer() {
        Properties props = new Properties();
        DebeziumEngine engine = new DebeziumEngine(props);
        
        assertThrows(IllegalStateException.class, engine::start);
    }
}
