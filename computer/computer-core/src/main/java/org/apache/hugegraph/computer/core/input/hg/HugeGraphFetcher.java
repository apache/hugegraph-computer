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

package org.apache.hugegraph.computer.core.input.hg;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.EdgeFetcher;
import org.apache.hugegraph.computer.core.input.GraphFetcher;
import org.apache.hugegraph.computer.core.input.InputSplit;
import org.apache.hugegraph.computer.core.input.VertexFetcher;
import org.apache.hugegraph.computer.core.rpc.InputSplitRpcService;
import org.apache.hugegraph.computer.core.util.HugeClientUtil;
import org.apache.hugegraph.driver.HugeClient;

public class HugeGraphFetcher implements GraphFetcher {

    private final HugeClient client;
    private final VertexFetcher vertexFetcher;
    private final EdgeFetcher edgeFetcher;
    private final InputSplitRpcService rpcService;

    public HugeGraphFetcher(Config config, InputSplitRpcService rpcService) {
        String url = config.get(ComputerOptions.HUGEGRAPH_URL);
        String graph = config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        String username = config.get(ComputerOptions.HUGEGRAPH_USERNAME);
        String password = config.get(ComputerOptions.HUGEGRAPH_PASSWORD);
        this.client = HugeClientUtil.newHugeClient(url, graph, username,
                                                   password);
        this.vertexFetcher = new HugeVertexFetcher(config, this.client);
        this.edgeFetcher = new HugeEdgeFetcher(config, this.client);
        this.rpcService = rpcService;
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public VertexFetcher vertexFetcher() {
        return this.vertexFetcher;
    }

    @Override
    public EdgeFetcher edgeFetcher() {
        return this.edgeFetcher;
    }

    @Override
    public InputSplit nextVertexInputSplit() {
        return this.rpcService.nextVertexInputSplit();
    }

    @Override
    public InputSplit nextEdgeInputSplit() {
        return this.rpcService.nextEdgeInputSplit();
    }
}
