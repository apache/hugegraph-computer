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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.BooleanValue;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.FloatValue;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.ListValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.NullValue;
import org.apache.hugegraph.computer.core.graph.value.StringValue;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class HugeConverterTest extends UnitTestBase {

    @Test
    public void testConvertId() {
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> HugeConverter.convertId(null));
        Assert.assertEquals(BytesId.of(1), HugeConverter.convertId((byte) 1));
        Assert.assertEquals(BytesId.of(1), HugeConverter.convertId((short) 1));
        Assert.assertEquals(BytesId.of(1), HugeConverter.convertId(1));
        Assert.assertEquals(BytesId.of(1L), HugeConverter.convertId(1L));
        Assert.assertEquals(BytesId.of("abc"), HugeConverter.convertId("abc"));
        UUID uuid = UUID.randomUUID();
        Assert.assertEquals(BytesId.of(uuid), HugeConverter.convertId(uuid));

        Assert.assertThrows(ComputerException.class,
                            () -> HugeConverter.convertId(true));
        Assert.assertThrows(ComputerException.class,
                            () -> HugeConverter.convertId(new byte[0]));
    }

    @Test
    public void testConvertValue() {
        Assert.assertEquals(NullValue.get(), HugeConverter.convertValue(null));
        Assert.assertEquals(new BooleanValue(true),
                            HugeConverter.convertValue(true));
        Assert.assertEquals(new IntValue(1), HugeConverter.convertValue(1));
        Assert.assertEquals(new LongValue(-1L),
                            HugeConverter.convertValue(-1L));
        Assert.assertEquals(new FloatValue(0.999F),
                            HugeConverter.convertValue(0.999F));
        Assert.assertEquals(new DoubleValue(-0.001D),
                            HugeConverter.convertValue(-0.001D));
        Assert.assertEquals(new StringValue("test"),
                            HugeConverter.convertValue("test"));
        ListValue<IntValue> listValue = new ListValue<>(ValueType.INT);
        listValue.add(new IntValue(1));
        listValue.add(new IntValue(2));
        List<Integer> list = ImmutableList.of(1, 2);
        Assert.assertEquals(listValue, HugeConverter.convertValue(list));

        ListValue<ListValue<LongValue>> nestListValue = new ListValue<>(
                                                        ValueType.LIST_VALUE);
        ListValue<LongValue> subListValue1 = new ListValue<>(ValueType.LONG);
        subListValue1.add(new LongValue(1L));
        subListValue1.add(new LongValue(2L));
        ListValue<LongValue> subListValue2 = new ListValue<>(ValueType.LONG);
        subListValue2.add(new LongValue(3L));
        subListValue2.add(new LongValue(4L));
        nestListValue.add(subListValue1);
        nestListValue.add(subListValue2);
        List<List<Long>> nestList = ImmutableList.of(
                                    ImmutableList.of(1L, 2L),
                                    ImmutableList.of(3L, 4L));
        Assert.assertEquals(nestListValue,
                            HugeConverter.convertValue(nestList));

        Assert.assertThrows(ComputerException.class,
                            () -> HugeConverter.convertValue(new byte[0]));
    }

    @Test
    public void testConvertProperties() {
        Map<String, Object> rawProperties = new HashMap<>();
        rawProperties.put("null-value", null);
        rawProperties.put("boolean-value", true);
        rawProperties.put("int-value", 1);
        rawProperties.put("long-value", 2L);
        rawProperties.put("float-value", 0.3F);
        rawProperties.put("double-value", 0.4D);
        rawProperties.put("list-value", ImmutableList.of(1, 2));

        Properties properties = graphFactory().createProperties();
        properties.put("null-value", NullValue.get());
        properties.put("boolean-value", new BooleanValue(true));
        properties.put("int-value", new IntValue(1));
        properties.put("long-value", new LongValue(2L));
        properties.put("float-value", new FloatValue(0.3F));
        properties.put("double-value", new DoubleValue(0.4D));
        ListValue<IntValue> listValue = new ListValue<>(ValueType.INT);
        listValue.add(new IntValue(1));
        listValue.add(new IntValue(2));
        properties.put("list-value", listValue);

        Assert.assertEquals(properties,
                            HugeConverter.convertProperties(rawProperties));
    }

    @Test
    public void testConvertEdgeNameWithLegacyFourPartEdgeId() {
        Edge edge = Mockito.mock(Edge.class);
        Mockito.when(edge.id()).thenReturn(
               "S1:178201>5>参数标准!3BA0>S4:239464");
        Mockito.when(edge.name()).thenReturn("stale_client_name");

        Assert.assertEquals("参数标准!3BA0",
                            HugeConverter.convertEdgeName(edge));
    }

    @Test
    public void testConvertEdgeNameWithFivePartEdgeId() {
        Edge edge = new Edge("belong_to_el_defect");
        edge.id("S1:178201>5>5>参数标准!3BA0>S4:239464");

        Assert.assertEquals("参数标准!3BA0",
                            HugeConverter.convertEdgeName(edge));
    }

    @Test
    public void testConvertEdgeNameWithSixPartEdgeId() {
        Edge edge = new Edge("belong_to_el_defect");
        edge.id("S1:178201>O>5>5>参数标准!3BA0>S4:239464");

        Assert.assertEquals("参数标准!3BA0",
                            HugeConverter.convertEdgeName(edge));
    }

    @Test
    public void testConvertEdgeNameWithSixPartInEdgeId() {
        Edge edge = new Edge("belong_to_el_defect");
        edge.id("S4:239464>I>5>5>参数标准!3BA0>S1:178201");

        Assert.assertEquals("参数标准!3BA0",
                            HugeConverter.convertEdgeName(edge));
    }

    @Test
    public void testConvertEdgeNameWithNullEdgeId() {
        Edge edge = Mockito.mock(Edge.class);
        Mockito.when(edge.id()).thenReturn(null);
        Mockito.when(edge.name()).thenReturn("fallback_name");

        Assert.assertEquals("fallback_name",
                            HugeConverter.convertEdgeName(edge));
    }

    @Test
    public void testConvertEdgeNameWithUnknownEdgeIdFormat() {
        Edge edge = Mockito.mock(Edge.class);
        Mockito.when(edge.id()).thenReturn(
               "S1:178201>bad>edge");
        Mockito.when(edge.name()).thenReturn("fallback_name");

        Assert.assertEquals("fallback_name",
                            HugeConverter.convertEdgeName(edge));

        Mockito.when(edge.id()).thenReturn(
               "S1:178201>O>5>5>参数标准!3BA0>S4:239464>extra");
        Assert.assertEquals("fallback_name",
                            HugeConverter.convertEdgeName(edge));
    }

    @Test
    public void testConvertEdgeNameWithNullEdge() {
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> HugeConverter.convertEdgeName(null));
    }
}
