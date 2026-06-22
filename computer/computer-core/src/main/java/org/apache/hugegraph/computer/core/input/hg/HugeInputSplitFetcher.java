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

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.InputSplit;
import org.apache.hugegraph.computer.core.input.InputSplitFetcher;
import org.apache.hugegraph.computer.core.util.HugeClientUtil;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.graph.Shard;
import org.apache.hugegraph.util.E;

public class HugeInputSplitFetcher implements InputSplitFetcher {

    private final Config config;
    private final HugeClient client;

    public HugeInputSplitFetcher(Config config) {
        this.config = config;
        String url = config.get(ComputerOptions.HUGEGRAPH_URL);
        String graph = config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        String username = config.get(ComputerOptions.HUGEGRAPH_USERNAME);
        String password = config.get(ComputerOptions.HUGEGRAPH_PASSWORD);
        int timeout = config.get(ComputerOptions.INPUT_SPLIT_FETCH_TIMEOUT);
        this.client = HugeClientUtil.newHugeClient(url, graph, username,
                                                   password, timeout);
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public List<InputSplit> fetchVertexInputSplits() {
        long splitSize = this.config.get(ComputerOptions.INPUT_SPLITS_SIZE);
        int maxSplits = this.config.get(ComputerOptions.INPUT_MAX_SPLITS);
        List<Shard> shards = this.client.traverser().vertexShards(splitSize);
        E.checkArgument(shards.size() <= maxSplits,
                        "Too many shards due to too small splitSize");
        List<InputSplit> splits = new ArrayList<>();
        for (Shard shard : shards) {
            InputSplit split = new InputSplit(shard.start(), shard.end());
            splits.add(split);
        }
        return splits;
    }

    @Override
    public List<InputSplit> fetchEdgeInputSplits() {
        long splitSize = this.config.get(ComputerOptions.INPUT_SPLITS_SIZE);
        int maxSplits = this.config.get(ComputerOptions.INPUT_MAX_SPLITS);
        List<Shard> shards = this.client.traverser().edgeShards(splitSize);
        E.checkArgument(shards.size() <= maxSplits,
                        "Too many shards due to too small splitSize");
        List<InputSplit> splits = new ArrayList<>();
        for (Shard shard : shards) {
            InputSplit split = new InputSplit(shard.start(), shard.end());
            splits.add(split);
        }
        return splits;
    }
}
