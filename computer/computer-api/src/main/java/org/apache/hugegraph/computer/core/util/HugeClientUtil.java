/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.computer.core.util;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.HugeClientBuilder;
import org.apache.hugegraph.rest.RestResult;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.util.JsonUtil;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBuilder;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;

public final class HugeClientUtil {

    private static final AtomicBoolean COMPATIBILITY_REGISTERED =
                                      new AtomicBoolean(false);

    public static HugeClient newHugeClient(String url, String graph,
                                           String username, String password) {
        registerCompatibilityModule();
        return new HugeClientBuilder(url, graph).configUser(username, password)
                                                .build();
    }

    public static HugeClient newHugeClient(String url, String graph,
                                           String username, String password,
                                           int timeout) {
        registerCompatibilityModule();
        return new HugeClientBuilder(url, graph).configUser(username, password)
                                                .configTimeout(timeout)
                                                .build();
    }

    public static void registerCompatibilityModule() {
        if (!COMPATIBILITY_REGISTERED.compareAndSet(false, true)) {
            return;
        }
        RestResult.registerModule(newCompatibilityModule());
        JsonUtil.registerModule(newCompatibilityModule());
    }

    private static SimpleModule newCompatibilityModule() {
        SimpleModule module = new SimpleModule(
                              "hugegraph-computer-client-compatibility");
        module.setDeserializerModifier(new BeanDeserializerModifier() {

            @Override
            public BeanDeserializerBuilder updateBuilder(
                   DeserializationConfig config, BeanDescription beanDesc,
                   BeanDeserializerBuilder builder) {
                if (EdgeLabel.class.equals(beanDesc.getBeanClass())) {
                    builder.addIgnorable("edgelabel_type");
                    builder.addIgnorable("parent_label");
                    builder.addIgnorable("links");
                }
                return builder;
            }
        });
        return module;
    }

    private HugeClientUtil() {
    }
}
