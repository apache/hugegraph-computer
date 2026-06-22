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

package org.apache.hugegraph.computer.core.output.hg.task;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.output.hg.exceptions.WriteBackException;
import org.apache.hugegraph.computer.core.output.hg.metrics.LoadSummary;
import org.apache.hugegraph.computer.core.output.hg.metrics.Printer;
import org.apache.hugegraph.computer.core.util.HugeClientUtil;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public final class TaskManager {

    private static final Logger LOG = Log.logger(TaskManager.class);

    public static final String BATCH_WORKER = "batch-worker-%d";
    public static final String SINGLE_WORKER = "single-worker-%d";

    private HugeClient client;
    private Config config;

    private final Semaphore batchSemaphore;
    private final Semaphore singleSemaphore;
    private final ExecutorService batchService;
    private final ExecutorService singleService;

    private LoadSummary loadSummary;

    public TaskManager(Config config) {
        this.config = config;
        String url = config.get(ComputerOptions.HUGEGRAPH_URL);
        String graph = config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        String username = config.get(ComputerOptions.HUGEGRAPH_USERNAME);
        String password = config.get(ComputerOptions.HUGEGRAPH_PASSWORD);
        this.client = HugeClientUtil.newHugeClient(url, graph, username,
                                                   password);
        // Try to make all batch threads running and don't wait for producer
        this.batchSemaphore = new Semaphore(this.batchSemaphoreNum());
        /*
         * Let batch threads go forward as far as possible and don't wait for
         * single thread pool
         */
        this.singleSemaphore = new Semaphore(this.singleSemaphoreNum());
        /*
         * In principle, unbounded synchronization queue(which may lead to OOM)
         * should not be used, but there the task manager uses semaphores to
         * limit the number of tasks added. When there are no idle threads in
         * the thread pool, the producer will be blocked, so OOM will not occur.
         */
        this.batchService = ExecutorUtil.newFixedThreadPool(
                            config.get(ComputerOptions.OUTPUT_BATCH_THREADS),
                            BATCH_WORKER);
        this.singleService = ExecutorUtil.newFixedThreadPool(
                             config.get(ComputerOptions.OUTPUT_SINGLE_THREADS),
                             SINGLE_WORKER);

        this.loadSummary = new LoadSummary();
        this.loadSummary.startTimer();
    }

    public HugeClient client() {
        return this.client;
    }

    private int batchSemaphoreNum() {
        return 1 + this.config.get(ComputerOptions.OUTPUT_BATCH_THREADS);
    }

    private int singleSemaphoreNum() {
        return 2 * this.config.get(ComputerOptions.OUTPUT_SINGLE_THREADS);
    }

    public void waitFinished() {
        LOG.info("Waiting for the insert tasks finished");
        try {
            // Wait batch mode task stopped
            this.batchSemaphore.acquire(this.batchSemaphoreNum());
            LOG.info("The batch-mode tasks stopped");
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting batch-mode tasks");
        } finally {
            this.batchSemaphore.release(this.batchSemaphoreNum());
        }

        try {
            // Wait single mode task stopped
            this.singleSemaphore.acquire(this.singleSemaphoreNum());
            LOG.info("The single-mode tasks stopped");
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting single-mode tasks");
        } finally {
            this.singleSemaphore.release(this.singleSemaphoreNum());
        }
    }

    public void shutdown() {
        long timeout = this.config.get(
                       ComputerOptions.OUTPUT_THREAD_POOL_SHUTDOWN_TIMEOUT);
        try {
            this.batchService.shutdown();
            this.batchService.awaitTermination(timeout, TimeUnit.SECONDS);
            LOG.info("The batch-mode tasks service executor shutdown");
        } catch (InterruptedException e) {
            LOG.error("The batch-mode tasks are interrupted");
        } finally {
            if (!this.batchService.isTerminated()) {
                LOG.error("The unfinished batch-mode tasks will be cancelled");
            }
            this.batchService.shutdownNow();
        }

        try {
            this.singleService.shutdown();
            this.singleService.awaitTermination(timeout, TimeUnit.SECONDS);
            LOG.info("The single-mode tasks service executor shutdown");
        } catch (InterruptedException e) {
            LOG.error("The single-mode tasks are interrupted");
        } finally {
            if (!this.singleService.isTerminated()) {
                LOG.error("The unfinished single-mode tasks will be cancelled");
            }
            this.singleService.shutdownNow();
        }
        this.loadSummary.stopTimer();
        Printer.printSummary(this.loadSummary);

        this.client.close();
    }

    public void submitBatch(List<Vertex> batch) {
        try {
            this.batchSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new WriteBackException(
                      "Interrupted while waiting to submit batch", e);
        }

        InsertTask task = new BatchInsertTask(this.config, this.client,
                                              batch, this.loadSummary);
        CompletableFuture.runAsync(task, this.batchService).exceptionally(e -> {
            LOG.warn("Batch insert error, try single insert", e);
            this.submitInSingle(batch);
            return null;
        }).whenComplete((r, e) -> this.batchSemaphore.release());
    }

    private void submitInSingle(List<Vertex> batch) {
        try {
            this.singleSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new WriteBackException(
                      "Interrupted while waiting to submit single", e);
        }

        InsertTask task = new SingleInsertTask(this.config, this.client,
                                               batch, this.loadSummary);
        CompletableFuture.runAsync(task, this.singleService)
                         .whenComplete((r, e) -> {
                             this.singleSemaphore.release();
                         });
    }
}
