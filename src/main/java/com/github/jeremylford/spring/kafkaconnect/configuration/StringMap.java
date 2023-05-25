package com.github.jeremylford.spring.kafkaconnect.configuration;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class StringMap implements Map<String, String> {

    private final Map<String, String> delegate;

    public StringMap() {
        this(new HashMap<>());
    }

    public StringMap(Map<String, String> delegate) {
        this.delegate = delegate;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public String get(Object key) {
        return delegate.get(key);
    }

    @Override
    public String put(String key, String value) {
        return delegate.put(key, value);
    }

    @Override
    public String remove(Object key) {
        return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        delegate.putAll(m);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set<String> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<String> values() {
        return delegate.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return delegate.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String getOrDefault(Object key, String defaultValue) {
        return delegate.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super String> action) {
        delegate.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super String, ? extends String> function) {
        delegate.replaceAll(function);
    }

    @Override
    public String putIfAbsent(String key, String value) {
        return delegate.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return delegate.remove(key, value);
    }

    @Override
    public boolean replace(String key, String oldValue, String newValue) {
        return delegate.replace(key, oldValue, newValue);
    }

    @Override
    public String replace(String key, String value) {
        return delegate.replace(key, value);
    }

    @Override
    public String computeIfAbsent(String key, Function<? super String, ? extends String> mappingFunction) {
        return delegate.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public String computeIfPresent(String key, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        return delegate.computeIfPresent(key, remappingFunction);
    }

    @Override
    public String compute(String key, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        return delegate.compute(key, remappingFunction);
    }

    @Override
    public String merge(String key, String value, BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        return delegate.merge(key, value, remappingFunction);
    }

    public void putInteger(String key, Integer value) {
        if (value != null) {
            put(key, String.valueOf(value));
        }
    }

    public void putShort(String key, Short value) {
        if (value != null) {
            put(key, String.valueOf(value));
        }
    }

    public void putLong(String key, Long value) {
        if (value != null) {
            put(key, String.valueOf(value));
        }
    }

    public void putDouble(String key, Double value) {
        if (value != null) {
            put(key, String.valueOf(value));
        }
    }

    public void putString(String key, String value) {
        if (value != null) {
            put(key, value);
        }
    }

    public void putBoolean(String key, boolean value) {
        put(key, String.valueOf(value));
    }

    public void putList(String key, List<String> values) {
        if (values != null && !values.isEmpty()) {
            put(key, String.join(",", values));
        }
    }

    public void putCollection(String key, Collection<String> values) {
        if (values != null && !values.isEmpty()) {
            put(key, String.join(",", values));
        }
    }
}
