package com.github.jeremylford.spring.kafkaconnect.configuration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StringMapTest {

    @Test
    public void putString() {
        StringMap map = new StringMap();
        map.putString("key", "value");

        assertTrue(map.containsKey("key"));
        assertEquals("value", map.get("key"));
    }
}
