package com.github.jeremylford.spring.kafkaconnect.configuration;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertySupportTest {

    @Test
    public void putString() {
        Map<String, String> map = new HashMap<>();
        PropertySupport.putString(map, "key", "value");

        assertTrue(map.containsKey("key"));
        assertEquals("value", map.get("key"));
    }
}
