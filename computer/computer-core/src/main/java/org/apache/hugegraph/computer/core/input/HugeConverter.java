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
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.computer.core.input;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.BooleanValue;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.FloatValue;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.ListValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.NullValue;
import org.apache.hugegraph.computer.core.graph.value.StringValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.SplicingIdGenerator;

public final class HugeConverter {

    private static final int LEGACY_EDGE_ID_PARTS = 4;
    private static final int DIRECTIONAL_EDGE_ID_PARTS = 6;

    private static final GraphFactory GRAPH_FACTORY =
                                      ComputerContext.instance().graphFactory();

    public static Id convertId(Object rawId) {
        E.checkArgumentNotNull(rawId, "The rawId can't be null");
        if (rawId instanceof Number) {
            return BytesId.of(((Number) rawId).longValue());
        } else if (rawId instanceof String) {
            return BytesId.of((String) rawId);
        } else if (rawId instanceof UUID) {
            return BytesId.of((UUID) rawId);
        } else {
            throw new ComputerException("Can't convert to Id from '%s'(%s)",
                                        rawId, rawId.getClass());
        }
    }

    public static Value.Tvalue<?> convertValue(Object rawValue) {
        if (rawValue == null) {
            return NullValue.get();
        } else if (rawValue instanceof Boolean) {
            return new BooleanValue((boolean) rawValue);
        } else if (rawValue instanceof Integer) {
            return new IntValue((int) rawValue);
        } else if (rawValue instanceof Long) {
            return new LongValue((long) rawValue);
        } else if (rawValue instanceof Float) {
            return new FloatValue((float) rawValue);
        } else if (rawValue instanceof Double) {
            return new DoubleValue((double) rawValue);
        } else if (rawValue instanceof String) {
            return new StringValue((String) rawValue);
        } else if (rawValue instanceof Collection) {
            @SuppressWarnings("unchecked")
            Collection<Object> collection = (Collection<Object>) rawValue;
            ListValue<Value.Tvalue<?>> listValue = new ListValue<>();
            for (Object nestedRawValue : collection) {
                listValue.add(convertValue(nestedRawValue));
            }
            return listValue;
        } else {
            throw new ComputerException("Can't convert to Value from '%s'(%s)",
                                        rawValue, rawValue.getClass());
        }
    }

    public static Properties convertProperties(
                             Map<String, Object> rawProperties) {
        Properties properties = GRAPH_FACTORY.createProperties();
        for (Map.Entry<String, Object> entry : rawProperties.entrySet()) {
            String key = entry.getKey();
            Value value = convertValue(entry.getValue());
            properties.put(key, value);
        }
        return properties;
    }

    public static String convertEdgeName(Edge edge) {
        E.checkArgumentNotNull(edge, "The edge can't be null");
        String edgeId = edge.id();
        if (edgeId == null) {
            return edge.name();
        }

        String[] parts = SplicingIdGenerator.split(edgeId);
        if (parts.length >= LEGACY_EDGE_ID_PARTS &&
            parts.length <= DIRECTIONAL_EDGE_ID_PARTS) {
            return parts[parts.length - 2];
        }
        return edge.name();
    }
}
