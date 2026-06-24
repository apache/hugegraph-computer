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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.computer.core.input.hg;

import java.lang.reflect.Method;

import org.apache.hugegraph.computer.core.util.HugeClientUtil;
import org.apache.hugegraph.rest.RestResult;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class HugeClientCompatibilityTest {

    @Test
    public void testReadEdgeLabelWithCurrentServerFields() {
        HugeClientUtil.registerCompatibilityModule();

        String content = "{" +
                         "\"id\":1," +
                         "\"name\":\"link\"," +
                         "\"edgelabel_type\":\"NORMAL\"," +
                         "\"source_label\":\"user\"," +
                         "\"target_label\":\"user\"," +
                         "\"links\":[{\"user\":\"user\"}]," +
                         "\"frequency\":\"SINGLE\"," +
                         "\"sort_keys\":[]," +
                         "\"nullable_keys\":[]," +
                         "\"index_labels\":[]," +
                         "\"properties\":[]," +
                         "\"status\":\"CREATED\"," +
                         "\"ttl\":0," +
                         "\"enable_label_index\":true," +
                         "\"user_data\":{\"~create_time\":\"2026-06-22 15:26:42.781\"}" +
                         "}";

        EdgeLabel edgeLabel = new RestResult(200, content, null).readObject(
                              EdgeLabel.class);

        Assert.assertEquals("link", edgeLabel.name());
        Assert.assertEquals("user", edgeLabel.sourceLabel());
        Assert.assertEquals("user", edgeLabel.targetLabel());
    }

    @Test
    public void testEdgeLabelCompatibilityUsesIgnoreUnknownMixin()
                     throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(newCompatibilityModule());

        Class<?> mixIn = mapper.getDeserializationConfig()
                               .findMixInClassFor(EdgeLabel.class);
        JsonIgnoreProperties annotation = mixIn.getAnnotation(
                                          JsonIgnoreProperties.class);

        Assert.assertTrue(annotation.ignoreUnknown());
    }

    private static SimpleModule newCompatibilityModule() throws Exception {
        Method method = HugeClientUtil.class.getDeclaredMethod(
                        "newCompatibilityModule");
        method.setAccessible(true);
        return (SimpleModule) method.invoke(null);
    }
}
