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
package com.github.jeremylford.spring.kafkaconnect;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.runtime.rest.resources.RootResource;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletProperties;
import org.glassfish.jersey.servlet.init.FilterUrlMappingsProviderImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import javax.ws.rs.ApplicationPath;

@ApplicationPath("/connect-api")
public class RESTConfiguration extends ResourceConfig {

    @Autowired
    public RESTConfiguration(Herder herder, WorkerConfig config) {
//        property(ServletProperties.FILTER_CONTEXT_PATH, "/connect-api/*");
        register(new JacksonJsonProvider());

        register(new RootResource(herder));
        register(new ConnectorsResource(herder, config));
        register(new ConnectorPluginsResource(herder));

        register(ConnectExceptionMapper.class);
        register(new FilterUrlMappingsProviderImpl());
    }

    @Bean
    public ConnectorExtensionManager connectorExtensionManager(Herder herder, WorkerConfig config) {
        return new ConnectorExtensionManager(herder, this, config);

    }
}
