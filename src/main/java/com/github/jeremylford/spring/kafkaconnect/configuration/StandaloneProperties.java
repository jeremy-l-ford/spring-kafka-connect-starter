/*
 * Copyright 2019 Jeremy Ford
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jeremylford.spring.kafkaconnect.configuration;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import java.util.HashMap;
import java.util.Map;

import static com.github.jeremylford.spring.kafkaconnect.configuration.PropertySupport.putString;

/**
 * PropertySupport defining a standalone kafka connect setup.
 */
public class StandaloneProperties {

    /**
     * File to store offset data in.
     */
    private String offsetStorageFileName;

    public String getOffsetStorageFileName() {
        return offsetStorageFileName;
    }

    public void setOffsetStorageFileName(String offsetStorageFileName) {
        this.offsetStorageFileName = offsetStorageFileName;
    }

    public Map<String, String> buildProperties() {

        Map<String, String> properties = new HashMap<>();
        putString(properties, StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, offsetStorageFileName);
        return properties;
    }

}
