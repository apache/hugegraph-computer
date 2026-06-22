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

package org.apache.hugegraph.computer.suite.unit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.id.IdFactory;
import org.apache.hugegraph.computer.core.graph.id.IdType;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.OutputFormat;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.io.StreamGraphInput;
import org.apache.hugegraph.computer.core.io.Readable;
import org.apache.hugegraph.computer.core.io.StreamGraphOutput;
import org.apache.hugegraph.computer.core.io.Writable;
import org.apache.hugegraph.computer.core.store.entry.EntryInput;
import org.apache.hugegraph.computer.core.store.entry.EntryInputImpl;
import org.apache.hugegraph.computer.core.util.HugeClientUtil;
import org.apache.hugegraph.computer.core.util.ComputerContextUtil;
import org.apache.hugegraph.computer.core.worker.MockComputationParams;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.logging.log4j.LogManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;

public class UnitTestBase {

    private static final Logger LOG = Log.logger(UnitTestBase.class);

    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                                        "0123456789" +
                                        "abcdefghijklmnopqrstuvxyz";
    private static String URL;
    private static String GRAPH;
    private static String USERNAME;
    private static String PASSWORD;
    private static HugeClient CLIENT = null;

    protected static void clearAll() {
        client().graphs().clearGraph(GRAPH, "I'm sure to delete all data");
    }

    @BeforeClass
    public static void init() throws ClassNotFoundException {
        Runtime.getRuntime().addShutdownHook(new Thread(LogManager::shutdown));

        LOG.info("Setup for UnitTestSuite of hugegraph-computer");

        String etcdUrl = System.getenv("BSP_ETCD_URL");
        if (StringUtils.isNotBlank(etcdUrl)) {
            Whitebox.setInternalState(ComputerOptions.BSP_ETCD_ENDPOINTS,
                                      "defaultValue", etcdUrl);
        }

        Whitebox.setInternalState(ComputerOptions.HUGEGRAPH_URL,
                "defaultValue",
                "http://127.0.0.1:8080");
        Whitebox.setInternalState(ComputerOptions.HUGEGRAPH_GRAPH_NAME,
                "defaultValue",
                "hugegraph");
        Whitebox.setInternalState(ComputerOptions.OUTPUT_HDFS_URL,
                "defaultValue",
                "hdfs://127.0.0.1:9000");
        Whitebox.setInternalState(ComputerOptions.OUTPUT_HDFS_USER,
                "defaultValue",
                System.getProperty("user.name"));
        Whitebox.setInternalState(ComputerOptions.OUTPUT_HDFS_KERBEROS_ENABLE,
                "defaultValue",
                false);
        Whitebox.setInternalState(ComputerOptions.OUTPUT_HDFS_KRB5_CONF,
                "defaultValue",
                "/etc/krb5.conf");
        Whitebox.setInternalState(ComputerOptions.OUTPUT_HDFS_KERBEROS_KEYTAB,
                "defaultValue",
                "");
        Whitebox.setInternalState(
                ComputerOptions.OUTPUT_HDFS_KERBEROS_PRINCIPAL,
                "defaultValue",
                "");
        Whitebox.setInternalState(
                ComputerOptions.INPUT_LOADER_SCHEMA_PATH,
                "defaultValue",
                "src/main/resources/hdfs_input_test/schema.json");
        Whitebox.setInternalState(
                ComputerOptions.INPUT_LOADER_STRUCT_PATH,
                "defaultValue",
                "src/main/resources/hdfs_input_test/struct.json");

        URL = ComputerOptions.HUGEGRAPH_URL.defaultValue();
        GRAPH = ComputerOptions.HUGEGRAPH_GRAPH_NAME.defaultValue();
        USERNAME = ComputerOptions.HUGEGRAPH_USERNAME.defaultValue();
        PASSWORD = ComputerOptions.HUGEGRAPH_PASSWORD.defaultValue();

        Class.forName(IdType.class.getName());
        // Don't forget to register options
        OptionSpace.register("computer",
                             "org.apache.hugegraph.computer.core.config.ComputerOptions");
        OptionSpace.register("computer-rpc", "org.apache.hugegraph.config.RpcOptions");

        UnitTestBase.updateOptions(
                ComputerOptions.ALGORITHM_RESULT_CLASS, LongValue.class.getName()
        );
    }

    @AfterClass
    public static void cleanup() {
        if (CLIENT != null) {
            CLIENT.close();
            CLIENT = null;
        }
    }

    public static void assertIdEqualAfterWriteAndRead(Id oldId)
                                                      throws IOException {
        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            oldId.write(bao);
            bytes = bao.toByteArray();
        }

        Id newId = IdFactory.createId(oldId.idType());
        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            newId.read(bai);
            Assert.assertEquals(oldId, newId);
        }
    }

    public static void assertValueEqualAfterWriteAndRead(Value oldValue)
                                                         throws IOException {
        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            oldValue.write(bao);
            bytes = bao.toByteArray();
        }

        Value newValue = graphFactory().createValue(oldValue.valueType());
        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            newValue.read(bai);
            Assert.assertEquals(oldValue, newValue);
        }
    }

    public static void assertEquals(double v1, double v2) {
        Assert.assertEquals(v1, v2, 1E-6);
    }

    public static void updateOptions(Object... optionKeyValues) {
        if (optionKeyValues == null || optionKeyValues.length == 0) {
            throw new ComputerException("Options can't be null or empty");
        }
        if ((optionKeyValues.length & 0x01) == 1) {
            throw new ComputerException("Options length must be even");
        }
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < optionKeyValues.length; i += 2) {
            Object key = optionKeyValues[i];
            E.checkArgument(key instanceof TypedOption,
                            "The option key must be TypedOption class");
            Object value = optionKeyValues[i + 1];
            E.checkArgument(value instanceof String,
                            "The option value must be String class");
            map.put(((TypedOption<?, ?>) key).name(), (String) value);
        }
        if (!map.keySet().contains(
                          ComputerOptions.ALGORITHM_PARAMS_CLASS.name())) {
            map.put(ComputerOptions.ALGORITHM_PARAMS_CLASS.name(),
                    MockComputationParams.class.getName());
        }
        ComputerContextUtil.initContext(map);
    }

    public static synchronized Config updateWithRequiredOptions(
                                      Object... options) {
        Object[] requiredOptions = new Object[] {
        };
        Object[] allOptions = new Object[requiredOptions.length +
                                         options.length];
        System.arraycopy(requiredOptions, 0, allOptions, 0,
                         requiredOptions.length);
        System.arraycopy(options, 0, allOptions,
                         requiredOptions.length, options.length);
        UnitTestBase.updateOptions(allOptions);
        return ComputerContext.instance().config();
    }

    public static void assertEqualAfterWriteAndRead(Writable writeObj,
                                                    Readable readObj)
                                                    throws IOException {
        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            writeObj.write(bao);
            bytes = bao.toByteArray();
        }

        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            readObj.read(bai);
            Assert.assertEquals(writeObj, readObj);
        }
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            // Ignore InterruptedException
        }
    }

    public static byte[] randomBytes(int size) {
        Random random = new Random();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) random.nextInt();
        }
        return bytes;
    }

    public static String randomString(int size) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return sb.toString();
    }

    protected static ComputerContext context() {
        return ComputerContext.instance();
    }

    protected static GraphFactory graphFactory() {
        return context().graphFactory();
    }

    protected static StreamGraphInput newStreamGraphInput(
                                      RandomAccessInput input) {
        EntryInput entryInput = new EntryInputImpl(input);
        return new StreamGraphInput(context(), entryInput);
    }

    protected static StreamGraphOutput newStreamGraphOutput(
                                       RandomAccessOutput output) {
        return (StreamGraphOutput) IOFactory.createGraphOutput(context(),
                                                               OutputFormat.BIN,
                                                               output);
    }

    protected static synchronized HugeClient client() {
        if (CLIENT == null) {
            CLIENT = HugeClientUtil.newHugeClient(URL, GRAPH, USERNAME,
                                                  PASSWORD);
        }
        return CLIENT;
    }

    public static boolean existError(Throwable[] exceptions) {
        boolean error = false;
        for (Throwable e : exceptions) {
            if (e != null) {
                error = true;
                LOG.warn("There exist error:", e);
                break;
            }
        }
        return error;
    }
}
